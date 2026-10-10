//! Processing Job Definition Parser
//!
//! Parses job definitions from YAML (converted to JSON internally) or JSON,
//! with the same rule as the workflow parser: a key the parser doesn't read,
//! a value of the wrong kind, an unknown operator type, or a duplicated key
//! is refused, naming the key and where it is.
//!
//! ```yaml
//! sources:
//!   - name: events-source
//!     stream:
//!       name: input-events
//!       namespace: production
//!       partitions: all
//!       batch_size: 100
//!   - name: cpu-metrics
//!     ts:
//!       measurement: cpu_usage
//!       namespace: production
//! sinks:
//!   - name: output
//!     stream:
//!       name: results
//!   - name: profiles
//!     kv:
//!       namespace: profiles
//!       key_prefix: user
//! ```
//!
//! ```zig
//! var diag: parser.Diagnostic = .{};
//! var def = try parser.parseJobDefinition(allocator, yaml_content, &diag);
//! defer def.deinit(allocator);
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const job_definition = @import("definition.zig");
const ExprFilterOperator = @import("operators/expr_filter.zig").ExprFilterOperator;
const yaml_to_json = @import("../util/yaml_to_json.zig");
const definition_diag = @import("../util/definition_diag.zig");

pub const Diagnostic = definition_diag.Diagnostic;

// Re-export types for convenience
pub const JobDefinition = job_definition.JobDefinition;
pub const SourceSpec = job_definition.SourceSpec;
pub const SourceKind = job_definition.SourceKind;
pub const SinkSpec = job_definition.SinkSpec;
pub const SinkKind = job_definition.SinkKind;
pub const OperatorSpec = job_definition.OperatorSpec;

// =============================================================================
// Parse Errors
// =============================================================================

pub const ParseError = error{
    MissingRequiredField,
    InvalidKind,
    InvalidFieldType,
    UnknownKey,
    DuplicateKey,
    UnknownOperatorType,
    MissingSource,
    MissingSink,
    MissingSourceStream,
    MissingSinkTarget,
    InvalidParallelism,
    InvalidPartitions,
    InvalidFormat,
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

fn optString(d: D, obj: JsonValue, key: []const u8) ParseError!?[]const u8 {
    const v = obj.object.get(key) orelse return null;
    return if (v == .string) v.string else wrongKind(d, key, "a string", v);
}

fn reqString(d: D, obj: JsonValue, key: []const u8, err: ParseError) ParseError![]const u8 {
    return try optString(d, obj, key) orelse d.fail(err, "missing required key \"{s}\"", .{key});
}

fn optInt(d: D, obj: JsonValue, key: []const u8) ParseError!?i64 {
    const v = obj.object.get(key) orelse return null;
    return if (v == .integer) v.integer else wrongKind(d, key, "an integer", v);
}

/// A positive integer key as `T`, or refused when it is zero, negative or
/// more than `T` holds.
fn optPositive(comptime T: type, d: D, obj: JsonValue, key: []const u8, err: ParseError) ParseError!?T {
    const v = try optInt(d, obj, key) orelse return null;
    if (v <= 0) return d.fail(err, "\"{s}\" must be at least 1, not {d}", .{ key, v });
    return std.math.cast(T, v) orelse d.fail(err, "\"{s}\" must be at most {d}, not {d}", .{ key, std.math.maxInt(T), v });
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

fn dupe(allocator: Allocator, s: []const u8) ParseError![]u8 {
    return allocator.dupe(u8, s) catch ParseError.OutOfMemory;
}

// =============================================================================
// Job Definition Parser
// =============================================================================

/// Parse a job definition from YAML or JSON. On a refusal other than
/// OutOfMemory, `diag` (when given) says what and where.
pub fn parseJobDefinition(allocator: Allocator, content: []const u8, diag: ?*Diagnostic) ParseError!JobDefinition {
    return parseJobDefinitionWithNamespace(allocator, content, null, diag);
}

/// Parse a job definition with an optional fallback namespace.
///
/// Resolution order for source/sink namespaces:
///   1. Explicit `namespace:` on the individual source/sink
///   2. Top-level `namespace:` in the definition
///   3. `fallback_namespace` (typically the command/job namespace)
///   4. `"default"`
pub fn parseJobDefinitionWithNamespace(allocator: Allocator, content: []const u8, fallback_namespace: ?[]const u8, diag: ?*Diagnostic) ParseError!JobDefinition {
    var scratch: Diagnostic = .{};
    const d = diag orelse &scratch;

    if (std.json.parseFromSlice(JsonValue, allocator, content, .{})) |parsed| {
        defer parsed.deinit();
        return parseJobDefinitionFromJson(allocator, parsed.value, fallback_namespace, d);
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

    return parseJobDefinitionFromJson(allocator, parsed.value, fallback_namespace, d);
}

fn parseJobDefinitionFromJson(allocator: Allocator, root: JsonValue, fallback_namespace: ?[]const u8, d: D) ParseError!JobDefinition {
    if (root != .object) return d.fail(ParseError.InvalidFormat, "a job definition must be a map, not {s}", .{kindName(root)});
    try checkKeys(d, root, &.{
        "kind",    "name",  "description", "namespace",     "parallelism", "batch_size",
        "sources", "sinks", "operators",   "checkpointing",
    });

    const kind = try reqString(d, root, "kind", ParseError.MissingRequiredField);
    if (!mem.eql(u8, kind, "Processing")) return d.fail(ParseError.InvalidKind, "\"kind\" must be Processing, not \"{s}\"", .{kind});

    const name = try optString(d, root, "name") orelse "unnamed-job";
    const description = try optString(d, root, "description") orelse "";
    const effective_namespace: []const u8 = try optString(d, root, "namespace") orelse (fallback_namespace orelse "default");
    const parallelism = try optPositive(u32, d, root, "parallelism", ParseError.InvalidParallelism) orelse 1;
    const batch_size = try optPositive(u32, d, root, "batch_size", ParseError.InvalidFormat) orelse 100;

    var checkpoint_interval_ms: ?u64 = null;
    if (try optObject(d, root, "checkpointing")) |cp_obj| {
        const mark = d.push("checkpointing");
        defer d.pop(mark);
        try checkKeys(d, cp_obj, &.{"interval_ms"});
        checkpoint_interval_ms = try optPositive(u64, d, cp_obj, "interval_ms", ParseError.InvalidFormat);
    }

    // Owns everything from here; "" frees as nothing.
    var def: JobDefinition = .{
        .name = "",
        .description = "",
        .namespace = "",
        .parallelism = parallelism,
        .batch_size = batch_size,
        .sources = .empty,
        .sinks = .empty,
        .operators = .empty,
        .checkpoint_interval_ms = checkpoint_interval_ms,
    };
    errdefer def.deinit(allocator);
    def.name = try dupe(allocator, name);
    def.description = try dupe(allocator, description);
    def.namespace = try dupe(allocator, effective_namespace);

    if (try optArray(d, root, "sources")) |arr| {
        const list_mark = d.push("sources");
        defer d.pop(list_mark);
        for (arr, 0..) |item, idx| {
            const mark = d.pushIndex(idx);
            defer d.pop(mark);
            try parseOneSource(allocator, d, item, idx, batch_size, effective_namespace, &def.sources);
        }
    }

    if (try optArray(d, root, "sinks")) |arr| {
        const list_mark = d.push("sinks");
        defer d.pop(list_mark);
        for (arr, 0..) |item, idx| {
            const mark = d.pushIndex(idx);
            defer d.pop(mark);
            try parseOneSink(allocator, d, item, idx, effective_namespace, &def.sinks);
        }
    }

    if (try optArray(d, root, "operators")) |arr| {
        const list_mark = d.push("operators");
        defer d.pop(list_mark);
        for (arr, 0..) |item, idx| {
            const mark = d.pushIndex(idx);
            defer d.pop(mark);
            try parseOperator(allocator, d, item, effective_namespace, &def.operators);
        }
    }

    if (def.sources.items.len == 0) return d.fail(ParseError.MissingSource, "a job needs at least one source", .{});
    if (def.sinks.items.len == 0) return d.fail(ParseError.MissingSink, "a job needs at least one sink", .{});

    return def;
}

// =============================================================================
// Operators
// =============================================================================

/// Each operator type and the keys it reads besides `type` and `name`.
/// `map` reads every other key as an output field.
const operator_keys = [_]struct { []const u8, ?[]const []const u8 }{
    .{ "filter", &.{"condition"} },
    .{ "passthrough", &.{} },
    .{ "keyby", &.{"key_expression"} },
    .{ "aggregate", &.{ "function", "field", "window", "window_size" } },
    .{ "map", null },
    .{ "flatmap", &.{ "array_field", "element_key" } },
    .{ "kv_lookup", &.{ "lookup_key", "namespace", "mode", "enrich_field" } },
    .{ "classify", &.{ "rules", "default_tag" } },
};

fn freeConfig(allocator: Allocator, entries: []const OperatorSpec.ConfigEntry) void {
    for (entries) |e| {
        allocator.free(e.key);
        allocator.free(e.value);
    }
}

fn appendConfig(allocator: Allocator, config: *std.ArrayList(OperatorSpec.ConfigEntry), key: []const u8, value: []const u8) ParseError!void {
    const k = try dupe(allocator, key);
    errdefer allocator.free(k);
    const v = try dupe(allocator, value);
    errdefer allocator.free(v);
    config.append(allocator, .{ .key = k, .value = v }) catch return ParseError.OutOfMemory;
}

fn parseOperator(allocator: Allocator, d: D, item: JsonValue, default_namespace: []const u8, operators: *std.ArrayList(OperatorSpec)) ParseError!void {
    if (item != .object) return d.fail(ParseError.InvalidFieldType, "an operator must be a map, not {s}", .{kindName(item)});
    const op_type = try reqString(d, item, "type", ParseError.MissingRequiredField);
    const op_name = try optString(d, item, "name") orelse op_type;

    const allowed: ?[]const []const u8 = inline for (operator_keys) |entry| {
        if (mem.eql(u8, op_type, entry[0])) break entry[1];
    } else return d.fail(
        ParseError.UnknownOperatorType,
        "unknown operator type \"{s}\" (filter|passthrough|keyby|aggregate|map|flatmap|kv_lookup|classify)",
        .{op_type},
    );

    try checkOperatorSettings(d, item, op_type);

    var config: std.ArrayList(OperatorSpec.ConfigEntry) = .empty;
    errdefer {
        freeConfig(allocator, config.items);
        config.deinit(allocator);
    }

    var it = item.object.iterator();
    next: while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (mem.eql(u8, key, "type") or mem.eql(u8, key, "name")) continue;
        if (allowed) |keys| {
            for (keys) |k| {
                if (mem.eql(u8, key, k)) break;
            } else return d.fail(ParseError.UnknownKey, "unknown key \"{s}\" for a {s} operator", .{ key, op_type });
        }
        if (mem.eql(u8, op_type, "classify") and mem.eql(u8, key, "rules")) continue :next;

        var num_buf: [64]u8 = undefined;
        const value: []const u8 = switch (entry.value_ptr.*) {
            .string => |s| s,
            .integer => |n| std.fmt.bufPrint(&num_buf, "{d}", .{n}) catch unreachable,
            .float => |f| std.fmt.bufPrint(&num_buf, "{d}", .{f}) catch unreachable,
            .bool => |b| if (b) "true" else "false",
            else => |v| return wrongKind(d, key, "a string, number or boolean", v),
        };
        try appendConfig(allocator, &config, key, value);
    }

    // A classify operator's `rules:` list becomes indexed condition_N/tag_N pairs.
    if (mem.eql(u8, op_type, "classify")) {
        if (try optArray(d, item, "rules")) |rules| {
            const rules_mark = d.push("rules");
            defer d.pop(rules_mark);
            for (rules, 0..) |rule, i| {
                const mark = d.pushIndex(i);
                defer d.pop(mark);
                if (rule != .object) return d.fail(ParseError.InvalidFieldType, "a rule must be a map, not {s}", .{kindName(rule)});
                try checkKeys(d, rule, &.{ "condition", "tag" });
                const cond = try reqString(d, rule, "condition", ParseError.MissingRequiredField);
                try checkCondition(d, cond);
                const tag = try reqString(d, rule, "tag", ParseError.MissingRequiredField);
                var key_buf: [32]u8 = undefined;
                try appendConfig(allocator, &config, std.fmt.bufPrint(&key_buf, "condition_{d}", .{i}) catch unreachable, cond);
                try appendConfig(allocator, &config, std.fmt.bufPrint(&key_buf, "tag_{d}", .{i}) catch unreachable, tag);
            }
        }
    }

    // A lookup that names no namespace reads the job's own, as an endpoint
    // does; resolved here so the running job and every replay of it read the
    // same one.
    if (mem.eql(u8, op_type, "kv_lookup") and !item.object.contains("namespace")) {
        try appendConfig(allocator, &config, "namespace", default_namespace);
    }

    const type_d = try dupe(allocator, op_type);
    errdefer allocator.free(type_d);
    const name_d = try dupe(allocator, op_name);
    errdefer allocator.free(name_d);
    const config_s: ?[]const OperatorSpec.ConfigEntry = if (config.items.len == 0) blk: {
        config.deinit(allocator);
        break :blk null;
    } else config.toOwnedSlice(allocator) catch return ParseError.OutOfMemory;
    errdefer if (config_s) |c| {
        freeConfig(allocator, c);
        allocator.free(c);
    };

    operators.append(allocator, .{ .type_name = type_d, .name = name_d, .config = config_s }) catch return ParseError.OutOfMemory;
}

/// A filter or classify condition the operator can't evaluate, refused here
/// rather than at pipeline creation.
fn checkCondition(d: D, cond: []const u8) ParseError!void {
    if (ExprFilterOperator.check(cond)) |why| return d.fail(ParseError.InvalidFieldType, "bad condition \"{s}\": {s}", .{ cond, why });
}

/// The settings each operator type needs, and the values its enums take,
/// refused here so `flo validate processing` refuses what submit refuses,
/// with a place. Keys are checked against `operator_keys` separately.
fn checkOperatorSettings(d: D, item: JsonValue, op_type: []const u8) ParseError!void {
    const need = struct {
        fn key(dd: D, it: JsonValue, k: []const u8) ParseError!void {
            if (!it.object.contains(k)) return dd.fail(ParseError.MissingRequiredField, "missing required key \"{s}\"", .{k});
        }
        fn oneOf(dd: D, it: JsonValue, k: []const u8, comptime names: []const []const u8) ParseError!void {
            const v = try optString(dd, it, k) orelse return;
            inline for (names) |n| {
                if (mem.eql(u8, v, n)) return;
            }
            const list = comptime blk: {
                var l: []const u8 = "";
                for (names, 0..) |n, i| l = l ++ (if (i == 0) "" else "|") ++ n;
                break :blk l;
            };
            return dd.fail(ParseError.InvalidFieldType, "\"{s}\" must be one of " ++ list ++ ", not \"{s}\"", .{ k, v });
        }
    };
    if (mem.eql(u8, op_type, "filter")) {
        try need.key(d, item, "condition");
        if (try optString(d, item, "condition")) |cond| try checkCondition(d, cond);
    } else if (mem.eql(u8, op_type, "keyby")) {
        try need.key(d, item, "key_expression");
    } else if (mem.eql(u8, op_type, "flatmap")) {
        try need.key(d, item, "array_field");
    } else if (mem.eql(u8, op_type, "kv_lookup")) {
        try need.key(d, item, "lookup_key");
        try need.oneOf(d, item, "mode", &.{ "filter", "enrich" });
    } else if (mem.eql(u8, op_type, "classify")) {
        try need.key(d, item, "rules");
    } else if (mem.eql(u8, op_type, "aggregate")) {
        try need.key(d, item, "function");
        try need.oneOf(d, item, "function", &.{ "sum", "count", "avg", "min", "max" });
        const function = (try optString(d, item, "function")).?;
        if (!mem.eql(u8, function, "count")) try need.key(d, item, "field");
        if (item.object.contains("window")) {
            try need.oneOf(d, item, "window", &.{ "tumbling", "count" });
            if (try optPositive(u64, d, item, "window_size", ParseError.InvalidFormat) == null)
                return d.fail(ParseError.MissingRequiredField, "missing required key \"window_size\"", .{});
        } else if (item.object.contains("window_size")) {
            return d.fail(ParseError.MissingRequiredField, "\"window_size\" needs \"window\"", .{});
        }
    }
}

// =============================================================================
// Sources
// =============================================================================

fn freeSource(allocator: Allocator, src: SourceSpec) void {
    allocator.free(src.name);
    allocator.free(src.stream);
    allocator.free(src.namespace);
    allocator.free(src.ts_measurement);
    allocator.free(src.ts_field);
    for (src.ts_tags) |t| allocator.free(t);
    allocator.free(src.ts_tags);
}

fn appendSource(allocator: Allocator, sources: *std.ArrayList(SourceSpec), src: SourceSpec) ParseError!void {
    sources.append(allocator, src) catch {
        freeSource(allocator, src);
        return ParseError.OutOfMemory;
    };
}

/// One source object: exactly one of `stream:` or `ts:`.
///
/// Stream source:
///   ```yaml
///   - name: events-source
///     stream:
///       name: input-events
///       namespace: production
///       partitions: all          # 2 | "0-63" | "0,3,7" | all (default)
///       batch_size: 100
///       poll_interval_ms: 1000
///   ```
///
/// TS source:
///   ```yaml
///   - name: cpu-metrics
///     ts:
///       measurement: cpu
///       namespace: production
///       tags:
///         host: web-01
///       field: usage_idle
///       poll_interval_ms: 500
///   ```
fn parseOneSource(allocator: Allocator, d: D, item: JsonValue, index: usize, default_batch_size: u32, default_namespace: []const u8, sources: *std.ArrayList(SourceSpec)) ParseError!void {
    if (item != .object) return d.fail(ParseError.InvalidFieldType, "a source must be a map, not {s}", .{kindName(item)});
    try checkKeys(d, item, &.{ "name", "stream", "ts" });

    // Default the source name to a per-index unique value so multiple unnamed
    // sources (e.g. flow-style `- stream: { name: … }`) don't collide.
    var name_buf: [32]u8 = undefined;
    const base_name = try optString(d, item, "name") orelse
        (std.fmt.bufPrint(&name_buf, "default-source-{d}", .{index}) catch unreachable);

    const ts_obj = try optObject(d, item, "ts");
    const stream_obj = try optObject(d, item, "stream");
    if (ts_obj != null and stream_obj != null) return d.fail(ParseError.InvalidFieldType, "a source has \"stream\" or \"ts\", not both", .{});
    if (ts_obj) |o| {
        const mark = d.push("ts");
        defer d.pop(mark);
        return appendTsSource(allocator, d, base_name, o, default_batch_size, default_namespace, sources);
    }
    if (stream_obj) |o| {
        const mark = d.push("stream");
        defer d.pop(mark);
        return appendStreamSource(allocator, d, base_name, o, default_batch_size, default_namespace, sources);
    }
    return d.fail(ParseError.MissingSourceStream, "a source needs \"stream\" or \"ts\"", .{});
}

/// A map whose keys are user names and whose values are strings, as flat
/// key/value pairs.
fn parseStringPairs(allocator: Allocator, d: D, obj: JsonValue, key: []const u8) ParseError![]const []const u8 {
    const map = try optObject(d, obj, key) orelse return &.{};
    const mark = d.push(key);
    defer d.pop(mark);

    const pairs = allocator.alloc([]const u8, map.object.count() * 2) catch return ParseError.OutOfMemory;
    var filled: usize = 0;
    errdefer {
        for (pairs[0..filled]) |s| allocator.free(s);
        allocator.free(pairs);
    }
    var it = map.object.iterator();
    while (it.next()) |entry| {
        const v = entry.value_ptr.*;
        if (v != .string) return wrongKind(d, entry.key_ptr.*, "a string", v);
        pairs[filled] = try dupe(allocator, entry.key_ptr.*);
        filled += 1;
        pairs[filled] = try dupe(allocator, v.string);
        filled += 1;
    }
    return pairs;
}

fn appendTsSource(
    allocator: Allocator,
    d: D,
    source_name: []const u8,
    ts_obj: JsonValue,
    default_batch_size: u32,
    default_namespace: []const u8,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    try checkKeys(d, ts_obj, &.{ "measurement", "namespace", "field", "tags", "batch_size", "poll_interval_ms" });
    const measurement = try reqString(d, ts_obj, "measurement", ParseError.MissingSourceStream);
    const ns = try optString(d, ts_obj, "namespace") orelse default_namespace;
    const field = try optString(d, ts_obj, "field") orelse "";
    const bs = try optPositive(u32, d, ts_obj, "batch_size", ParseError.InvalidFormat) orelse default_batch_size;
    const poll_ms = try optPositive(u32, d, ts_obj, "poll_interval_ms", ParseError.InvalidFormat) orelse 1000;

    var src: SourceSpec = .{ .kind = .ts, .name = "", .stream = "", .namespace = "", .partition = 0, .batch_size = bs, .ts_poll_interval_ms = poll_ms };
    errdefer freeSource(allocator, src);
    src.name = try dupe(allocator, source_name);
    src.namespace = try dupe(allocator, ns);
    src.ts_measurement = try dupe(allocator, measurement);
    src.ts_field = try dupe(allocator, field);
    src.ts_tags = try parseStringPairs(allocator, d, ts_obj, "tags");
    try appendSource(allocator, sources, src);
}

fn appendStreamSource(
    allocator: Allocator,
    d: D,
    source_name: []const u8,
    stream_obj: JsonValue,
    default_batch_size: u32,
    default_namespace: []const u8,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    try checkKeys(d, stream_obj, &.{ "name", "namespace", "partitions", "batch_size", "poll_interval_ms" });
    const stream_name = try reqString(d, stream_obj, "name", ParseError.MissingSourceStream);
    const ns = try optString(d, stream_obj, "namespace") orelse default_namespace;
    const bs = try optPositive(u32, d, stream_obj, "batch_size", ParseError.InvalidFormat) orelse default_batch_size;
    const poll_ms = try optPositive(u32, d, stream_obj, "poll_interval_ms", ParseError.InvalidFormat) orelse 1000;

    const partitions = stream_obj.object.get("partitions") orelse JsonValue{ .string = "all" };
    switch (partitions) {
        .integer => |v| {
            const p = std.math.cast(u32, v) orelse return d.fail(ParseError.InvalidPartitions, "\"partitions\" must be a partition number, a range like 0-63, a list like 0,3,7 or all, not {d}", .{v});
            try appendStreamSpec(allocator, sources, source_name, null, stream_name, ns, p, bs, poll_ms);
        },
        .string => |s| try expandPartitions(allocator, d, source_name, stream_name, ns, bs, poll_ms, s, sources),
        else => |v| return wrongKind(d, "partitions", "a partition number, a range, a list or all", v),
    }
}

/// Expand a `partitions:` string into one SourceSpec per partition:
///   - `"all"`   → one entry with partition = PARTITION_ALL (the handler resolves it)
///   - `"0-63"`  → range, inclusive (64 entries)
///   - `"0,3,7"` → list (3 entries)
fn expandPartitions(
    allocator: Allocator,
    d: D,
    base_name: []const u8,
    stream_name: []const u8,
    ns: []const u8,
    bs: u32,
    poll_ms: u32,
    partitions_str: []const u8,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    if (mem.eql(u8, partitions_str, "all")) {
        return appendStreamSpec(allocator, sources, base_name, null, stream_name, ns, job_definition.PARTITION_ALL, bs, poll_ms);
    }
    const bad = "\"partitions\" must be a partition number, a range like 0-63, a list like 0,3,7 or all, not \"{s}\"";

    if (mem.indexOfScalar(u8, partitions_str, '-')) |dash_pos| {
        const start = std.fmt.parseInt(u32, partitions_str[0..dash_pos], 10) catch return d.fail(ParseError.InvalidPartitions, bad, .{partitions_str});
        const end = std.fmt.parseInt(u32, partitions_str[dash_pos + 1 ..], 10) catch return d.fail(ParseError.InvalidPartitions, bad, .{partitions_str});
        if (start > end) return d.fail(ParseError.InvalidPartitions, bad, .{partitions_str});
        var p = start;
        while (true) : (p += 1) {
            try appendStreamSpec(allocator, sources, base_name, p, stream_name, ns, p, bs, poll_ms);
            if (p == end) break;
        }
        return;
    }

    var iter = mem.splitScalar(u8, partitions_str, ',');
    var count: u32 = 0;
    while (iter.next()) |seg| {
        const trimmed = mem.trim(u8, seg, " ");
        if (trimmed.len == 0) continue;
        const p = std.fmt.parseInt(u32, trimmed, 10) catch return d.fail(ParseError.InvalidPartitions, bad, .{partitions_str});
        try appendStreamSpec(allocator, sources, base_name, p, stream_name, ns, p, bs, poll_ms);
        count += 1;
    }
    if (count == 0) return d.fail(ParseError.InvalidPartitions, bad, .{partitions_str});
}

/// One stream SourceSpec; an expanded partition gets a "-p<N>" name suffix.
fn appendStreamSpec(
    allocator: Allocator,
    sources: *std.ArrayList(SourceSpec),
    base_name: []const u8,
    suffix_partition: ?u32,
    stream_name: []const u8,
    ns: []const u8,
    partition: u32,
    bs: u32,
    poll_ms: u32,
) ParseError!void {
    var src: SourceSpec = .{ .name = "", .stream = "", .namespace = "", .partition = partition, .batch_size = bs, .ts_poll_interval_ms = poll_ms };
    errdefer freeSource(allocator, src);
    src.name = if (suffix_partition) |p|
        std.fmt.allocPrint(allocator, "{s}-p{d}", .{ base_name, p }) catch return ParseError.OutOfMemory
    else
        try dupe(allocator, base_name);
    src.stream = try dupe(allocator, stream_name);
    src.namespace = try dupe(allocator, ns);
    try appendSource(allocator, sources, src);
}

// =============================================================================
// Sinks
// =============================================================================

fn freeSink(allocator: Allocator, snk: SinkSpec) void {
    allocator.free(snk.name);
    allocator.free(snk.target);
    allocator.free(snk.namespace);
    allocator.free(snk.key_prefix);
    allocator.free(snk.separator);
    allocator.free(snk.write_mode);
    if (snk.match) |tags| {
        for (tags) |t| allocator.free(t);
        allocator.free(tags);
    }
    allocator.free(snk.ts_measurement);
    allocator.free(snk.ts_value_field);
    for (snk.ts_tag_keys) |k| allocator.free(k);
    allocator.free(snk.ts_tag_keys);
    for (snk.ts_field_keys) |k| allocator.free(k);
    allocator.free(snk.ts_field_keys);
}

/// One sink object: exactly one of `kv:`, `queue:`, `ts:` or `stream:`, and
/// optionally `match:` (a tag, or a list of tags all of which a record must
/// carry).
fn parseOneSink(allocator: Allocator, d: D, item: JsonValue, index: usize, default_namespace: []const u8, sinks: *std.ArrayList(SinkSpec)) ParseError!void {
    if (item != .object) return d.fail(ParseError.InvalidFieldType, "a sink must be a map, not {s}", .{kindName(item)});
    try checkKeys(d, item, &.{ "name", "match", "kv", "queue", "ts", "stream" });

    // Default the sink name to a per-index unique value so multiple unnamed
    // sinks don't trip the duplicate-name check.
    var name_buf: [32]u8 = undefined;
    const sink_name = try optString(d, item, "name") orelse
        (std.fmt.bufPrint(&name_buf, "default-sink-{d}", .{index}) catch unreachable);

    const kinds = [_][]const u8{ "kv", "queue", "ts", "stream" };
    var kind_key: ?[]const u8 = null;
    for (kinds) |k| {
        if (!item.object.contains(k)) continue;
        if (kind_key) |first| return d.fail(ParseError.InvalidFieldType, "a sink has one of kv, queue, ts or stream, not both \"{s}\" and \"{s}\"", .{ first, k });
        kind_key = k;
    }
    const kk = kind_key orelse return d.fail(ParseError.MissingSinkTarget, "a sink needs one of kv, queue, ts or stream", .{});
    const obj = (try optObject(d, item, kk)).?;

    var snk: SinkSpec = .{
        .name = "",
        .kind = .stream,
        .target = "",
        .namespace = "",
        .key_prefix = "",
        .separator = "",
        .write_mode = "",
        .ttl_ms = null,
        .priority = 0,
        .delay_ms = null,
        .use_key_as_dedup = true,
    };
    errdefer freeSink(allocator, snk);
    snk.name = try dupe(allocator, sink_name);
    snk.match = try parseMatch(allocator, d, item);
    // Only upsert exists; the field stays for the sink writer.
    snk.write_mode = try dupe(allocator, "upsert");
    snk.separator = try dupe(allocator, ":");

    const mark = d.push(kk);
    defer d.pop(mark);
    if (mem.eql(u8, kk, "kv")) {
        try checkKeys(d, obj, &.{ "namespace", "key_prefix", "separator", "ttl_ms" });
        snk.kind = .kv;
        snk.namespace = try dupe(allocator, try optString(d, obj, "namespace") orelse default_namespace);
        snk.key_prefix = try dupe(allocator, try optString(d, obj, "key_prefix") orelse "");
        if (try optString(d, obj, "separator")) |sep| {
            allocator.free(snk.separator);
            snk.separator = "";
            snk.separator = try dupe(allocator, sep);
        }
        if (try optPositive(u64, d, obj, "ttl_ms", ParseError.InvalidFormat)) |ttl| {
            // A TTL is applied in nanoseconds; one that can't be is refused here.
            if (ttl > std.math.maxInt(u64) / std.time.ns_per_ms) return d.fail(ParseError.InvalidFormat, "\"ttl_ms\" must be at most {d}, not {d}", .{ std.math.maxInt(u64) / std.time.ns_per_ms, ttl });
            snk.ttl_ms = ttl;
        }
    } else if (mem.eql(u8, kk, "queue")) {
        try checkKeys(d, obj, &.{ "name", "namespace", "priority", "use_key_as_dedup" });
        snk.kind = .queue;
        snk.target = try dupe(allocator, try optString(d, obj, "name") orelse sink_name);
        snk.namespace = try dupe(allocator, try optString(d, obj, "namespace") orelse default_namespace);
        if (try optInt(d, obj, "priority")) |p| {
            snk.priority = std.math.cast(u8, p) orelse return d.fail(ParseError.InvalidFormat, "\"priority\" must be from 0 to 255, not {d}", .{p});
        }
        snk.use_key_as_dedup = try optBool(d, obj, "use_key_as_dedup") orelse true;
    } else if (mem.eql(u8, kk, "ts")) {
        try checkKeys(d, obj, &.{ "measurement", "namespace", "value_field", "tags", "fields" });
        snk.kind = .ts;
        const measurement = try optString(d, obj, "measurement") orelse sink_name;
        snk.target = try dupe(allocator, measurement);
        snk.ts_measurement = try dupe(allocator, measurement);
        snk.namespace = try dupe(allocator, try optString(d, obj, "namespace") orelse default_namespace);
        snk.ts_value_field = try dupe(allocator, try optString(d, obj, "value_field") orelse "value");
        snk.ts_tag_keys = try parseStringPairs(allocator, d, obj, "tags");
        snk.ts_field_keys = try parseStringPairs(allocator, d, obj, "fields");
    } else {
        try checkKeys(d, obj, &.{ "name", "namespace" });
        snk.kind = .stream;
        snk.target = try dupe(allocator, try reqString(d, obj, "name", ParseError.MissingSinkTarget));
        snk.namespace = try dupe(allocator, try optString(d, obj, "namespace") orelse default_namespace);
    }

    sinks.append(allocator, snk) catch return ParseError.OutOfMemory;
}

/// `match:` as a list of tags (a single tag is sugar for a one-tag list).
fn parseMatch(allocator: Allocator, d: D, item: JsonValue) ParseError!?[]const []const u8 {
    const v = item.object.get("match") orelse return null;
    const mark = d.push("match");
    defer d.pop(mark);
    switch (v) {
        .string => |s| {
            const list = allocator.alloc([]const u8, 1) catch return ParseError.OutOfMemory;
            errdefer allocator.free(list);
            list[0] = try dupe(allocator, s);
            return list;
        },
        .array => |arr| {
            if (arr.items.len == 0) return null;
            const list = allocator.alloc([]const u8, arr.items.len) catch return ParseError.OutOfMemory;
            var filled: usize = 0;
            errdefer {
                for (list[0..filled]) |t| allocator.free(t);
                allocator.free(list);
            }
            for (arr.items, 0..) |elem, i| {
                if (elem != .string) {
                    const im = d.pushIndex(i);
                    defer d.pop(im);
                    return d.fail(ParseError.InvalidFormat, "a tag must be a string, not {s}", .{kindName(elem)});
                }
                list[i] = try dupe(allocator, elem.string);
                filled += 1;
            }
            return list;
        },
        else => {
            d.pop(mark);
            return wrongKind(d, "match", "a tag or a list of tags", v);
        },
    }
}

// =============================================================================
// Tests
// =============================================================================

test "parser: nested YAML full definition" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\name: my-pipeline
        \\sources:
        \\  - name: events-source
        \\    stream:
        \\      name: input-events
        \\      namespace: production
        \\      partitions: 2
        \\sinks:
        \\  - name: output
        \\    stream:
        \\      name: output-events
        \\      namespace: analytics
        \\parallelism: 4
        \\batch_size: 500
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("my-pipeline", def.name);
    try std.testing.expectEqual(@as(u32, 4), def.parallelism);
    try std.testing.expectEqual(@as(u32, 500), def.batch_size);

    try std.testing.expectEqual(@as(usize, 1), def.sources.items.len);
    const src = def.primarySource().?;
    try std.testing.expectEqualStrings("input-events", src.stream);
    try std.testing.expectEqualStrings("production", src.namespace);
    try std.testing.expectEqual(@as(u32, 2), src.partition);
    try std.testing.expectEqual(@as(u32, 500), src.batch_size);

    try std.testing.expectEqual(@as(usize, 1), def.sinks.items.len);
    const snk = def.primarySink().?;
    try std.testing.expectEqualStrings("output-events", snk.target);
    try std.testing.expectEqualStrings("analytics", snk.namespace);
    try std.testing.expect(snk.kind == .stream);
}

test "parser: nested YAML minimal with defaults" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("unnamed-job", def.name);
    try std.testing.expectEqual(@as(u32, 1), def.parallelism);
    try std.testing.expectEqual(@as(u32, 100), def.batch_size);

    const src = def.primarySource().?;
    try std.testing.expectEqualStrings("events", src.stream);
    try std.testing.expectEqualStrings("default", src.namespace);
    try std.testing.expectEqual(job_definition.PARTITION_ALL, src.partition);
    try std.testing.expectEqual(@as(u32, 100), src.batch_size);

    const snk = def.primarySink().?;
    try std.testing.expectEqualStrings("results", snk.target);
    try std.testing.expectEqualStrings("default", snk.namespace);
    try std.testing.expect(snk.kind == .stream);
}

test "parser: multi-source array" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\name: multi-src-job
        \\sources:
        \\  - name: clicks
        \\    stream:
        \\      name: click-events
        \\      namespace: prod
        \\      partitions: 1
        \\      batch_size: 250
        \\  - name: impressions
        \\    stream:
        \\      name: impression-events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), def.sources.items.len);

    try std.testing.expectEqualStrings("clicks", def.sources.items[0].name);
    try std.testing.expectEqualStrings("click-events", def.sources.items[0].stream);
    try std.testing.expectEqualStrings("prod", def.sources.items[0].namespace);
    try std.testing.expectEqual(@as(u32, 1), def.sources.items[0].partition);
    try std.testing.expectEqual(@as(u32, 250), def.sources.items[0].batch_size);

    try std.testing.expectEqualStrings("impressions", def.sources.items[1].name);
    try std.testing.expectEqualStrings("impression-events", def.sources.items[1].stream);
    try std.testing.expectEqualStrings("default", def.sources.items[1].namespace);
    try std.testing.expectEqual(@as(u32, 100), def.sources.items[1].batch_size);
}

// =============================================================================
// Stream Source Object Form Tests
// =============================================================================

test "parser: stream source object form full" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\name: obj-stream-job
        \\sources:
        \\  - name: events-source
        \\    stream:
        \\      name: input-events
        \\      namespace: production
        \\      partitions: 2
        \\      batch_size: 500
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), def.sources.items.len);
    const src = def.primarySource().?;
    try std.testing.expectEqualStrings("events-source", src.name);
    try std.testing.expectEqualStrings("input-events", src.stream);
    try std.testing.expectEqualStrings("production", src.namespace);
    try std.testing.expectEqual(@as(u32, 2), src.partition);
    try std.testing.expectEqual(@as(u32, 500), src.batch_size);
    try std.testing.expect(src.kind == .stream);
}

test "parser: stream source object form minimal" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const src = def.primarySource().?;
    try std.testing.expectEqualStrings("events", src.stream);
    try std.testing.expectEqualStrings("default", src.namespace);
    try std.testing.expectEqual(job_definition.PARTITION_ALL, src.partition);
    try std.testing.expectEqual(@as(u32, 100), src.batch_size);
}

test "parser: stream source object form with partitions range" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: clicks
        \\    stream:
        \\      name: click-events
        \\      namespace: prod
        \\      partitions: "0-2"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), def.sources.items.len);
    try std.testing.expectEqualStrings("clicks-p0", def.sources.items[0].name);
    try std.testing.expectEqualStrings("click-events", def.sources.items[0].stream);
    try std.testing.expectEqualStrings("prod", def.sources.items[0].namespace);
    try std.testing.expectEqual(@as(u32, 0), def.sources.items[0].partition);
}

test "parser: stream source object form with partitions all" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: full
        \\    stream:
        \\      name: txn-events
        \\      partitions: "all"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), def.sources.items.len);
    try std.testing.expectEqual(job_definition.PARTITION_ALL, def.sources.items[0].partition);
    try std.testing.expectEqualStrings("txn-events", def.sources.items[0].stream);
}

test "parser: stream object form missing name" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      namespace: ns
        \\sinks:
        \\  - stream:
        \\      name: out
    ;
    try std.testing.expectError(error.MissingSourceStream, parseJobDefinition(allocator, text, null));
}

test "parser: multi-sink array (stream + kv + queue)" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\name: multi-sink-job
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: output
        \\    stream:
        \\      name: results
        \\      namespace: analytics
        \\  - name: profiles
        \\    kv:
        \\      namespace: user-store
        \\      key_prefix: user
        \\      separator: ":"
        \\      ttl_ms: 86400000
        \\  - name: tasks
        \\    queue:
        \\      name: task-queue
        \\      namespace: work
        \\      priority: 5
        \\      use_key_as_dedup: false
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), def.sinks.items.len);

    // Stream sink
    const s0 = def.sinks.items[0];
    try std.testing.expect(s0.kind == .stream);
    try std.testing.expectEqualStrings("output", s0.name);
    try std.testing.expectEqualStrings("results", s0.target);
    try std.testing.expectEqualStrings("analytics", s0.namespace);

    // KV sink
    const s1 = def.sinks.items[1];
    try std.testing.expect(s1.kind == .kv);
    try std.testing.expectEqualStrings("profiles", s1.name);
    try std.testing.expectEqualStrings("user-store", s1.namespace);
    try std.testing.expectEqualStrings("user", s1.key_prefix);
    try std.testing.expectEqualStrings(":", s1.separator);
    try std.testing.expectEqualStrings("upsert", s1.write_mode);
    try std.testing.expectEqual(@as(?u64, 86400000), s1.ttl_ms);

    // Queue sink
    const s2 = def.sinks.items[2];
    try std.testing.expect(s2.kind == .queue);
    try std.testing.expectEqualStrings("tasks", s2.name);
    try std.testing.expectEqualStrings("task-queue", s2.target);
    try std.testing.expectEqualStrings("work", s2.namespace);
    try std.testing.expectEqual(@as(u8, 5), s2.priority);
    try std.testing.expectEqual(@as(?u64, null), s2.delay_ms);
    try std.testing.expectEqual(false, s2.use_key_as_dedup);
}

test "parser: nested YAML with operator list" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\name: filtered-pipeline
        \\sources:
        \\  - stream:
        \\      name: raw-events
        \\sinks:
        \\  - stream:
        \\      name: clean-events
        \\operators:
        \\  - type: filter
        \\    name: positive
        \\    condition: not_empty
        \\  - type: map
        \\    name: transform
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("filtered-pipeline", def.name);
    try std.testing.expectEqual(@as(usize, 2), def.operators.items.len);
    try std.testing.expectEqualStrings("filter", def.operators.items[0].type_name);
    try std.testing.expectEqualStrings("positive", def.operators.items[0].name);
    try std.testing.expectEqualStrings("map", def.operators.items[1].type_name);
    try std.testing.expectEqualStrings("transform", def.operators.items[1].name);
}

test "parser: missing source" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sinks:
        \\  - stream:
        \\      name: output
    ;
    try std.testing.expectError(error.MissingSource, parseJobDefinition(allocator, text, null));
}

test "parser: missing sink" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: input
    ;
    try std.testing.expectError(error.MissingSink, parseJobDefinition(allocator, text, null));
}

test "parser: source without stream field" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: no-stream
        \\sinks:
        \\  - stream:
        \\      name: out
    ;
    try std.testing.expectError(error.MissingSourceStream, parseJobDefinition(allocator, text, null));
}

test "parser: sink without target" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: in
        \\sinks:
        \\  - name: no-target
    ;
    try std.testing.expectError(error.MissingSinkTarget, parseJobDefinition(allocator, text, null));
}

test "parser: invalid parallelism" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: in
        \\sinks:
        \\  - stream:
        \\      name: out
        \\parallelism: abc
    ;
    try std.testing.expectError(error.InvalidFieldType, parseJobDefinition(allocator, text, null));
}

test "parser: zero parallelism rejected" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: in
        \\sinks:
        \\  - stream:
        \\      name: out
        \\parallelism: 0
    ;
    try std.testing.expectError(error.InvalidParallelism, parseJobDefinition(allocator, text, null));
}

test "parser: empty text" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.MissingRequiredField, parseJobDefinition(allocator, "", null));
}

test "parser: comments and blank lines" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\# Pipeline configuration
        \\
        \\sources:
        \\  - stream:
        \\      name: events
        \\# Sink config
        \\sinks:
        \\  - stream:
        \\      name: results
        \\
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("events", def.primarySource().?.stream);
    try std.testing.expectEqualStrings("results", def.primarySink().?.target);
}

test "parser: no operators field yields empty list" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), def.operators.items.len);
}

test "parser: JSON input (native)" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "kind": "Processing",
        \\  "name": "json-job",
        \\  "sources": [{ "stream": { "name": "in", "namespace": "prod" } }],
        \\  "sinks": [{ "stream": { "name": "out" } }],
        \\  "parallelism": 2,
        \\  "operators": [
        \\    { "type": "map", "name": "xform" }
        \\  ]
        \\}
    ;

    var def = try parseJobDefinition(allocator, json, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("json-job", def.name);
    try std.testing.expectEqualStrings("in", def.primarySource().?.stream);
    try std.testing.expectEqualStrings("prod", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("out", def.primarySink().?.target);
    try std.testing.expectEqual(@as(u32, 2), def.parallelism);
    try std.testing.expectEqual(@as(usize, 1), def.operators.items.len);
    try std.testing.expectEqualStrings("map", def.operators.items[0].type_name);
    try std.testing.expectEqualStrings("xform", def.operators.items[0].name);
}

test "parser: JSON multi-source multi-sink" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "kind": "Processing",
        \\  "name": "json-multi",
        \\  "sources": [
        \\    { "name": "s1", "stream": { "name": "stream-a" } },
        \\    { "name": "s2", "stream": { "name": "stream-b", "namespace": "ns2" } }
        \\  ],
        \\  "sinks": [
        \\    { "name": "out1", "stream": { "name": "out-stream" } },
        \\    { "name": "out2", "kv": { "namespace": "kv-ns", "key_prefix": "pfx" } }
        \\  ]
        \\}
    ;

    var def = try parseJobDefinition(allocator, json, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), def.sources.items.len);
    try std.testing.expectEqualStrings("stream-a", def.sources.items[0].stream);
    try std.testing.expectEqualStrings("stream-b", def.sources.items[1].stream);
    try std.testing.expectEqualStrings("ns2", def.sources.items[1].namespace);

    try std.testing.expectEqual(@as(usize, 2), def.sinks.items.len);
    try std.testing.expect(def.sinks.items[0].kind == .stream);
    try std.testing.expectEqualStrings("out-stream", def.sinks.items[0].target);
    try std.testing.expect(def.sinks.items[1].kind == .kv);
    try std.testing.expectEqualStrings("kv-ns", def.sinks.items[1].namespace);
    try std.testing.expectEqualStrings("pfx", def.sinks.items[1].key_prefix);
}

test "parser: operator without explicit name uses type" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
        \\operators:
        \\  - type: passthrough
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), def.operators.items.len);
    try std.testing.expectEqualStrings("passthrough", def.operators.items[0].type_name);
    try std.testing.expectEqualStrings("passthrough", def.operators.items[0].name);
}

test "parser: missing kind field" {
    const allocator = std.testing.allocator;

    const text =
        \\name: no-kind
        \\sources:
        \\  - stream:
        \\      name: in
        \\sinks:
        \\  - stream:
        \\      name: out
    ;
    try std.testing.expectError(error.MissingRequiredField, parseJobDefinition(allocator, text, null));
}

test "parser: invalid kind rejected" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Workflow
        \\name: wrong-kind
        \\sources:
        \\  - stream:
        \\      name: in
        \\sinks:
        \\  - stream:
        \\      name: out
    ;
    try std.testing.expectError(error.InvalidKind, parseJobDefinition(allocator, text, null));
}

test "parser: source batch_size overrides top-level" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\batch_size: 200
        \\sources:
        \\  - name: fast
        \\    stream:
        \\      name: fast-events
        \\      batch_size: 500
        \\  - name: slow
        \\    stream:
        \\      name: slow-events
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 500), def.sources.items[0].batch_size);
    try std.testing.expectEqual(@as(u32, 200), def.sources.items[1].batch_size);
}

test "parser: KV sink defaults" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: store
        \\    kv:
        \\      namespace: my-ns
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const snk = def.primarySink().?;
    try std.testing.expect(snk.kind == .kv);
    try std.testing.expectEqualStrings("my-ns", snk.namespace);
    try std.testing.expectEqualStrings("", snk.key_prefix);
    try std.testing.expectEqualStrings(":", snk.separator);
    try std.testing.expectEqualStrings("upsert", snk.write_mode);
    try std.testing.expect(snk.ttl_ms == null);
}

// =============================================================================
// Partition Range Tests
// =============================================================================

test "parser: partitions range expands to multiple sources" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: clicks
        \\    stream:
        \\      name: click-events
        \\      partitions: "0-3"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 4), def.sources.items.len);
    try std.testing.expectEqualStrings("clicks-p0", def.sources.items[0].name);
    try std.testing.expectEqual(@as(u32, 0), def.sources.items[0].partition);
    try std.testing.expectEqualStrings("clicks-p1", def.sources.items[1].name);
    try std.testing.expectEqual(@as(u32, 1), def.sources.items[1].partition);
    try std.testing.expectEqualStrings("clicks-p2", def.sources.items[2].name);
    try std.testing.expectEqual(@as(u32, 2), def.sources.items[2].partition);
    try std.testing.expectEqualStrings("clicks-p3", def.sources.items[3].name);
    try std.testing.expectEqual(@as(u32, 3), def.sources.items[3].partition);

    // All share stream name
    for (def.sources.items) |src| {
        try std.testing.expectEqualStrings("click-events", src.stream);
    }
}

test "parser: partitions comma list" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: sel
        \\    stream:
        \\      name: events
        \\      partitions: "0,5,10"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), def.sources.items.len);
    try std.testing.expectEqual(@as(u32, 0), def.sources.items[0].partition);
    try std.testing.expectEqual(@as(u32, 5), def.sources.items[1].partition);
    try std.testing.expectEqual(@as(u32, 10), def.sources.items[2].partition);
    try std.testing.expectEqualStrings("sel-p0", def.sources.items[0].name);
    try std.testing.expectEqualStrings("sel-p5", def.sources.items[1].name);
    try std.testing.expectEqualStrings("sel-p10", def.sources.items[2].name);
}

test "parser: partitions all sentinel" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: full
        \\    stream:
        \\      name: txn-events
        \\      partitions: "all"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), def.sources.items.len);
    try std.testing.expectEqualStrings("full", def.sources.items[0].name);
    try std.testing.expectEqual(job_definition.PARTITION_ALL, def.sources.items[0].partition);
    try std.testing.expectEqualStrings("txn-events", def.sources.items[0].stream);
}

test "parser: partitions with batch_size" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\batch_size: 200
        \\sources:
        \\  - name: fast
        \\    stream:
        \\      name: events
        \\      partitions: "0-1"
        \\      batch_size: 500
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), def.sources.items.len);
    try std.testing.expectEqual(@as(u32, 500), def.sources.items[0].batch_size);
    try std.testing.expectEqual(@as(u32, 500), def.sources.items[1].batch_size);
}

test "parser: invalid partitions range" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\      partitions: "5-2"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;
    try std.testing.expectError(error.InvalidPartitions, parseJobDefinition(allocator, text, null));
}

test "parser: partitions mixed with single partition source" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - name: single
        \\    stream:
        \\      name: a
        \\      partitions: 7
        \\  - name: multi
        \\    stream:
        \\      name: b
        \\      partitions: "0-2"
        \\sinks:
        \\  - stream:
        \\      name: out
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    // 1 single + 3 expanded = 4 total
    try std.testing.expectEqual(@as(usize, 4), def.sources.items.len);
    try std.testing.expectEqualStrings("single", def.sources.items[0].name);
    try std.testing.expectEqual(@as(u32, 7), def.sources.items[0].partition);
    try std.testing.expectEqualStrings("multi-p0", def.sources.items[1].name);
    try std.testing.expectEqual(@as(u32, 0), def.sources.items[1].partition);
}

// =============================================================================
// Tag-Based Routing Tests
// =============================================================================

test "parser: sink with match" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: main-output
        \\    stream:
        \\      name: results
        \\  - name: late-events
        \\    stream:
        \\      name: late-data
        \\    match:
        \\      - late
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), def.sinks.items.len);
    try std.testing.expect(def.sinks.items[0].match == null);
    const tag_list = def.sinks.items[1].match.?;
    try std.testing.expectEqual(@as(usize, 1), tag_list.len);
    try std.testing.expectEqualStrings("late", tag_list[0]);
}

test "parser: KV sink with match" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: errors
        \\    kv:
        \\      namespace: error-store
        \\    match:
        \\      - error-records
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const snk = def.primarySink().?;
    try std.testing.expect(snk.kind == .kv);
    const tag_list = snk.match.?;
    try std.testing.expectEqual(@as(usize, 1), tag_list.len);
    try std.testing.expectEqualStrings("error-records", tag_list[0]);
}

test "parser: queue sink with match" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: dlq
        \\    queue:
        \\      name: dead-letter
        \\    match:
        \\      - failures
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const snk = def.primarySink().?;
    try std.testing.expect(snk.kind == .queue);
    const tag_list = snk.match.?;
    try std.testing.expectEqual(@as(usize, 1), tag_list.len);
    try std.testing.expectEqualStrings("failures", tag_list[0]);
    try std.testing.expectEqualStrings("dead-letter", snk.target);
}

test "parser: sink with multiple match tags (AND match)" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: urgent
        \\    stream:
        \\      name: urgent-stream
        \\    match:
        \\      - late
        \\      - high-value
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const snk = def.primarySink().?;
    const tag_list = snk.match.?;
    try std.testing.expectEqual(@as(usize, 2), tag_list.len);
    try std.testing.expectEqualStrings("late", tag_list[0]);
    try std.testing.expectEqualStrings("high-value", tag_list[1]);
}

// =============================================================================
// KV Lookup Operator Tests
// =============================================================================

test "parser: kv_lookup operator with all config" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
        \\operators:
        \\  - type: kv_lookup
        \\    name: check-account
        \\    lookup_key: "account:${$.account_id}"
        \\    namespace: production
        \\    mode: filter
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), def.operators.items.len);
    const op = def.operators.items[0];
    try std.testing.expectEqualStrings("kv_lookup", op.type_name);
    try std.testing.expectEqualStrings("check-account", op.name);

    // Verify config entries parsed correctly
    try std.testing.expect(op.config != null);
    try std.testing.expect(op.getConfig("lookup_key") != null);
    try std.testing.expectEqualStrings("account:${$.account_id}", op.getConfig("lookup_key").?);
    try std.testing.expectEqualStrings("production", op.getConfig("namespace").?);
    try std.testing.expectEqualStrings("filter", op.getConfig("mode").?);
}

test "parser: kv_lookup operator with enrich mode" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: enriched
        \\operators:
        \\  - type: kv_lookup
        \\    name: enrich-user
        \\    lookup_key: "user/${$.user_id}"
        \\    mode: enrich
        \\    enrich_field: user_data
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const op = def.operators.items[0];
    try std.testing.expectEqualStrings("kv_lookup", op.type_name);
    try std.testing.expectEqualStrings("user/${$.user_id}", op.getConfig("lookup_key").?);
    try std.testing.expectEqualStrings("enrich", op.getConfig("mode").?);
    try std.testing.expectEqualStrings("user_data", op.getConfig("enrich_field").?);
}

test "parser: kv_lookup operator minimal config" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
        \\operators:
        \\  - type: kv_lookup
        \\    name: simple-lookup
        \\    lookup_key: "${$.id}"
    ;

    var def = try parseJobDefinitionWithNamespace(allocator, text, "acme", null);
    defer def.deinit(allocator);

    const op = def.operators.items[0];
    try std.testing.expectEqualStrings("kv_lookup", op.type_name);
    try std.testing.expectEqualStrings("${$.id}", op.getConfig("lookup_key").?);
    // No namespace named: the lookup reads the job's own, as endpoints do.
    try std.testing.expectEqualStrings("acme", op.getConfig("namespace").?);
    try std.testing.expect(op.getConfig("mode") == null);
}

// =============================================================================
// Namespace inheritance tests
// =============================================================================

test "parser: top-level namespace inherited by sources and sinks" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\namespace: production
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("production", def.namespace);
    try std.testing.expectEqualStrings("production", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("production", def.primarySink().?.namespace);
}

test "parser: source/sink namespace overrides top-level" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\namespace: production
        \\sources:
        \\  - stream:
        \\      name: events
        \\      namespace: staging
        \\sinks:
        \\  - name: out
        \\    kv:
        \\      namespace: analytics
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("production", def.namespace);
    try std.testing.expectEqualStrings("staging", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("analytics", def.primarySink().?.namespace);
}

test "parser: fallback namespace from command" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    // No top-level namespace in YAML, but command provides "my-team"
    var def = try parseJobDefinitionWithNamespace(allocator, text, "my-team", null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("my-team", def.namespace);
    try std.testing.expectEqualStrings("my-team", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("my-team", def.primarySink().?.namespace);
}

test "parser: YAML namespace takes priority over fallback" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\namespace: from-yaml
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    // YAML has namespace "from-yaml", command provides "from-command"
    var def = try parseJobDefinitionWithNamespace(allocator, text, "from-command", null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("from-yaml", def.namespace);
    try std.testing.expectEqualStrings("from-yaml", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("from-yaml", def.primarySink().?.namespace);
}

test "parser: namespace inheritance with TS source and TS sink" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\namespace: monitoring
        \\sources:
        \\  - name: cpu
        \\    ts:
        \\      measurement: cpu_usage
        \\sinks:
        \\  - name: metrics-out
        \\    ts:
        \\      measurement: processed_cpu
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("monitoring", def.namespace);
    try std.testing.expectEqualStrings("monitoring", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("monitoring", def.primarySink().?.namespace);
}

test "parser: namespace inheritance with queue sink" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\namespace: team-x
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - name: dlq
        \\    queue:
        \\      name: dead-letter
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("team-x", def.namespace);
    try std.testing.expectEqualStrings("team-x", def.primarySink().?.namespace);
}

test "parser: no namespace anywhere defaults to 'default'" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: results
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("default", def.namespace);
    try std.testing.expectEqualStrings("default", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("default", def.primarySink().?.namespace);
}

test "parser: classify operator with rules array" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: errors
        \\    match:
        \\      - errors
        \\  - stream:
        \\      name: all-events
        \\operators:
        \\  - type: classify
        \\    name: route-errors
        \\    rules:
        \\      - condition: value_contains:error
        \\        tag: errors
        \\      - condition: value_contains:warn
        \\        tag: warnings
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), def.operators.items.len);
    const op = def.operators.items[0];
    try std.testing.expectEqualStrings("classify", op.type_name);
    try std.testing.expectEqualStrings("route-errors", op.name);

    // rules: array should be expanded to indexed config pairs
    try std.testing.expect(op.config != null);
    const cfg = op.config.?;
    try std.testing.expectEqual(@as(usize, 4), cfg.len);
    try std.testing.expectEqualStrings("condition_0", cfg[0].key);
    try std.testing.expectEqualStrings("value_contains:error", cfg[0].value);
    try std.testing.expectEqualStrings("tag_0", cfg[1].key);
    try std.testing.expectEqualStrings("errors", cfg[1].value);
    try std.testing.expectEqualStrings("condition_1", cfg[2].key);
    try std.testing.expectEqualStrings("value_contains:warn", cfg[2].value);
    try std.testing.expectEqualStrings("tag_1", cfg[3].key);
    try std.testing.expectEqualStrings("warnings", cfg[3].value);

    // sinks: first sink should have match, second should be firehose
    try std.testing.expectEqual(@as(usize, 2), def.sinks.items.len);
    try std.testing.expect(def.sinks.items[0].match != null);
    try std.testing.expectEqual(@as(usize, 1), def.sinks.items[0].match.?.len);
    try std.testing.expectEqualStrings("errors", def.sinks.items[0].match.?[0]);
    try std.testing.expect(def.sinks.items[1].match == null);
}

test "parser: classify operator with rules array and inline config" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: out
        \\operators:
        \\  - type: classify
        \\    name: tagger
        \\    default_tag: other
        \\    rules:
        \\      - condition: value_contains:critical
        \\        tag: critical
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const op = def.operators.items[0];
    try std.testing.expect(op.config != null);
    const cfg = op.config.?;
    // 1 inline config entry (default_tag) + 2 from rules (condition_0, tag_0)
    try std.testing.expectEqual(@as(usize, 3), cfg.len);
    try std.testing.expectEqualStrings("default_tag", cfg[0].key);
    try std.testing.expectEqualStrings("other", cfg[0].value);
    try std.testing.expectEqualStrings("condition_0", cfg[1].key);
    try std.testing.expectEqualStrings("value_contains:critical", cfg[1].value);
    try std.testing.expectEqualStrings("tag_0", cfg[2].key);
    try std.testing.expectEqualStrings("critical", cfg[2].value);
}

test "parser: classify operator with json conditions" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: payments
        \\    match:
        \\      - payments
        \\  - stream:
        \\      name: transfers
        \\    match:
        \\      - transfers
        \\  - stream:
        \\      name: high-value
        \\    match:
        \\      - high-value
        \\  - stream:
        \\      name: all-events
        \\operators:
        \\  - type: classify
        \\    name: route-json
        \\    rules:
        \\      - condition: "json:type^=payment"
        \\        tag: payments
        \\      - condition: "json:type*=transfer"
        \\        tag: transfers
        \\      - condition: "json:amount>10000"
        \\        tag: high-value
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const op = def.operators.items[0];
    try std.testing.expectEqualStrings("classify", op.type_name);
    try std.testing.expectEqualStrings("route-json", op.name);

    const cfg = op.config.?;
    try std.testing.expectEqual(@as(usize, 6), cfg.len);
    try std.testing.expectEqualStrings("condition_0", cfg[0].key);
    try std.testing.expectEqualStrings("json:type^=payment", cfg[0].value);
    try std.testing.expectEqualStrings("tag_0", cfg[1].key);
    try std.testing.expectEqualStrings("payments", cfg[1].value);
    try std.testing.expectEqualStrings("condition_1", cfg[2].key);
    try std.testing.expectEqualStrings("json:type*=transfer", cfg[2].value);
    try std.testing.expectEqualStrings("tag_1", cfg[3].key);
    try std.testing.expectEqualStrings("transfers", cfg[3].value);
    try std.testing.expectEqualStrings("condition_2", cfg[4].key);
    try std.testing.expectEqualStrings("json:amount>10000", cfg[4].value);
    try std.testing.expectEqualStrings("tag_2", cfg[5].key);
    try std.testing.expectEqualStrings("high-value", cfg[5].value);

    // 4 sinks: 3 tagged + 1 firehose
    try std.testing.expectEqual(@as(usize, 4), def.sinks.items.len);
    try std.testing.expect(def.sinks.items[0].match != null);
    try std.testing.expectEqualStrings("payments", def.sinks.items[0].match.?[0]);
    try std.testing.expect(def.sinks.items[1].match != null);
    try std.testing.expectEqualStrings("transfers", def.sinks.items[1].match.?[0]);
    try std.testing.expect(def.sinks.items[2].match != null);
    try std.testing.expectEqualStrings("high-value", def.sinks.items[2].match.?[0]);
    try std.testing.expect(def.sinks.items[3].match == null); // firehose
}

test "parser: classify operator with default tag" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: events
        \\sinks:
        \\  - stream:
        \\      name: errors
        \\    match:
        \\      - errors
        \\  - stream:
        \\      name: other
        \\    match:
        \\      - other
        \\operators:
        \\  - type: classify
        \\    name: route-default
        \\    default_tag: other
        \\    rules:
        \\      - condition: "value_contains:error"
        \\        tag: errors
    ;

    var def = try parseJobDefinition(allocator, text, null);
    defer def.deinit(allocator);

    const op = def.operators.items[0];
    try std.testing.expectEqualStrings("classify", op.type_name);
    try std.testing.expectEqualStrings("route-default", op.name);

    const cfg = op.config.?;
    // Should have: default_tag=other, condition_0, tag_0
    var has_default = false;
    for (cfg) |entry| {
        if (std.mem.eql(u8, entry.key, "default_tag")) {
            try std.testing.expectEqualStrings("other", entry.value);
            has_default = true;
        }
    }
    try std.testing.expect(has_default);
}

test "parser: a stream or queue name holding a NUL, or too long for its namespace, is refused at submit" {
    const allocator = std.testing.allocator;
    for ([_][]const u8{
        \\{"kind":"Processing","name":"j","sources":[{"stream":{"name":"in\u0000x"}}],"sinks":[{"stream":{"name":"out"}}]}
        ,
        \\{"kind":"Processing","name":"j","sources":[{"stream":{"name":"in"}}],"sinks":[{"queue":{"name":"q\u0000x"}}]}
    }) |json_def| {
        var def = try parseJobDefinitionWithNamespace(allocator, json_def, "acme", null);
        defer def.deinit(allocator);
        try std.testing.expectEqualStrings("stream or queue name must not contain NUL", def.namespaceRefusal().?);
    }
    const long = "s" ** 4096;
    const too_long = "{\"kind\":\"Processing\",\"name\":\"j\",\"sources\":[{\"stream\":{\"name\":\"" ++ long ++ "\"}}],\"sinks\":[{\"stream\":{\"name\":\"out\"}}]}";
    var def = try parseJobDefinitionWithNamespace(allocator, too_long, "acme", null);
    defer def.deinit(allocator);
    try std.testing.expectEqualStrings("stream or queue name too long for its namespace", def.namespaceRefusal().?);
}

/// The diagnostic a job definition is refused with.
fn expectRefused(content: []const u8, expected: ParseError, message: []const u8) !void {
    var diag: Diagnostic = .{};
    try std.testing.expectError(expected, parseJobDefinition(std.testing.allocator, content, &diag));
    try std.testing.expectEqualStrings(message, diag.message());
}

fn job(comptime extra: []const u8, comptime source: []const u8, comptime sink: []const u8) []const u8 {
    return
    \\{ "kind": "Processing", "name": "j",
    ++ extra ++
        \\  "sources": [ { "stream": { "name": "in"
    ++ source ++
        \\ } } ],
        \\  "sinks": [ { "stream": { "name": "out" }
    ++ sink ++
        \\ } ] }
    ;
}

test "parser: an unknown key is refused by name and place" {
    try expectRefused(job("\"paralelism\": 2,", "", ""), ParseError.UnknownKey, "unknown key \"paralelism\" at the top level");
    try expectRefused(job("", ", \"batchSize\": 5", ""), ParseError.UnknownKey, "unknown key \"batchSize\" at sources[0].stream");
    try expectRefused(job("", "", ", \"matches\": \"x\""), ParseError.UnknownKey, "unknown key \"matches\" at sinks[0]");
    try expectRefused(job("\"checkpointing\": { \"interval\": 5 },", "", ""), ParseError.UnknownKey, "unknown key \"interval\" at checkpointing");
    try expectRefused(
        \\{ "kind": "Processing", "sources": [ { "stream": { "name": "in" } } ],
        \\  "sinks": [ { "kv": { "write_mode": "versioned" } } ] }
    , ParseError.UnknownKey, "unknown key \"write_mode\" at sinks[0].kv");
    try expectRefused(job("\"operators\": [ { \"type\": \"filter\", \"condition\": \"key_not_empty\", \"conditon\": \"x\" } ],", "", ""), ParseError.UnknownKey, "unknown key \"conditon\" for a filter operator at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"map\", \"module\": \"x.wasm\", \"a\": \"$.b\" }, { \"type\": \"passthrough\", \"module\": \"m\" } ],", "", ""), ParseError.UnknownKey, "unknown key \"module\" for a passthrough operator at operators[1]");
    try expectRefused(job("\"operators\": [ { \"type\": \"classify\", \"rules\": [ { \"condition\": \"key_not_empty\", \"tag\": \"t\", \"tags\": \"u\" } ] } ],", "", ""), ParseError.UnknownKey, "unknown key \"tags\" at operators[0].rules[0]");
}

test "parser: an unknown operator type is refused at parse" {
    try expectRefused(job("\"operators\": [ { \"type\": \"fliter\", \"condition\": \"key_not_empty\" } ],", "", ""), ParseError.UnknownOperatorType, "unknown operator type \"fliter\" (filter|passthrough|keyby|aggregate|map|flatmap|kv_lookup|classify) at operators[0]");
}

test "parser: a value of the wrong kind or out of range is refused, not ignored" {
    try expectRefused(job("", ", \"batch_size\": 0", ""), ParseError.InvalidFormat, "\"batch_size\" must be at least 1, not 0 at sources[0].stream");
    try expectRefused(job("", ", \"poll_interval_ms\": \"fast\"", ""), ParseError.InvalidFieldType, "\"poll_interval_ms\" must be an integer, not a string at sources[0].stream");
    try expectRefused(job("\"checkpointing\": { \"interval_ms\": -1 },", "", ""), ParseError.InvalidFormat, "\"interval_ms\" must be at least 1, not -1 at checkpointing");
    try expectRefused(job("\"operators\": [ \"filter\" ],", "", ""), ParseError.InvalidFieldType, "an operator must be a map, not a string at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"map\", \"out\": { \"nested\": 1 } } ],", "", ""), ParseError.InvalidFieldType, "\"out\" must be a string, number or boolean, not a map at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"classify\", \"rules\": [ { \"condition\": \"key_not_empty\" } ] } ],", "", ""), ParseError.MissingRequiredField, "missing required key \"tag\" at operators[0].rules[0]");
    try expectRefused(
        \\{ "kind": "Processing", "sources": [ { "stream": { "name": "in" } } ],
        \\  "sinks": [ { "queue": { "name": "q", "priority": 300 } } ] }
    , ParseError.InvalidFormat, "\"priority\" must be from 0 to 255, not 300 at sinks[0].queue");
    try expectRefused(
        \\{ "kind": "Processing", "sources": [ { "stream": { "name": "in" } } ],
        \\  "sinks": [ { "queue": { "name": "q", "delay_ms": 1000 } } ] }
    , ParseError.UnknownKey, "unknown key \"delay_ms\" at sinks[0].queue");
    try expectRefused(
        \\{ "kind": "Processing", "sources": [ { "ts": { "measurement": "m", "tags": { "host": 1 } } } ],
        \\  "sinks": [ { "stream": { "name": "out" } } ] }
    , ParseError.InvalidFieldType, "\"host\" must be a string, not an integer at sources[0].ts.tags");
    try expectRefused(
        \\{ "kind": "Processing", "sources": [ { "stream": { "name": "in" }, "ts": { "measurement": "m" } } ],
        \\  "sinks": [ { "stream": { "name": "out" } } ] }
    , ParseError.InvalidFieldType, "a source has \"stream\" or \"ts\", not both at sources[0]");
    try expectRefused(
        \\{ "kind": "Processing", "sources": [ { "stream": { "name": "in" } } ],
        \\  "sinks": [ { "stream": { "name": "out" }, "kv": {} } ] }
    , ParseError.InvalidFieldType, "a sink has one of kv, queue, ts or stream, not both \"kv\" and \"stream\" at sinks[0]");
}

test "parser: a duplicated key is refused by name" {
    try expectRefused(
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: in
        \\      name: other
        \\sinks:
        \\  - stream:
        \\      name: out
    , ParseError.DuplicateKey, "key \"name\" appears twice at sources[0].stream");
}

test "parser: an operator missing what it needs, or with an unknown mode, is refused at parse" {
    try expectRefused(job("\"operators\": [ { \"type\": \"filter\" } ],", "", ""), ParseError.MissingRequiredField, "missing required key \"condition\" at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"aggregate\", \"function\": \"median\", \"field\": \"$.a\" } ],", "", ""), ParseError.InvalidFieldType, "\"function\" must be one of sum|count|avg|min|max, not \"median\" at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"aggregate\", \"function\": \"sum\" } ],", "", ""), ParseError.MissingRequiredField, "missing required key \"field\" at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"aggregate\", \"function\": \"count\", \"window\": \"sliding\", \"window_size\": 5 } ],", "", ""), ParseError.InvalidFieldType, "\"window\" must be one of tumbling|count, not \"sliding\" at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"aggregate\", \"function\": \"count\", \"window\": \"count\", \"window_size\": 0 } ],", "", ""), ParseError.InvalidFormat, "\"window_size\" must be at least 1, not 0 at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"kv_lookup\", \"lookup_key\": \"k\", \"mode\": \"join\" } ],", "", ""), ParseError.InvalidFieldType, "\"mode\" must be one of filter|enrich, not \"join\" at operators[0]");
}

test "parser: a filter or classify condition the operator can't evaluate is refused at parse" {
    try expectRefused(job("\"operators\": [ { \"type\": \"filter\", \"condition\": \"valeu_contains:x\" } ],", "", ""), ParseError.InvalidFieldType, "bad condition \"valeu_contains:x\": unknown condition at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"filter\", \"condition\": \"\" } ],", "", ""), ParseError.InvalidFieldType, "bad condition \"\": the condition is empty at operators[0]");
    try expectRefused(job("\"operators\": [ { \"type\": \"classify\", \"rules\": [ { \"condition\": \"not_empty\", \"tag\": \"a\" }, { \"condition\": \"json:amount>lots\", \"tag\": \"b\" } ] } ],", "", ""), ParseError.InvalidFieldType, "bad condition \"json:amount>lots\": a json: >, >=, < or <= needs a finite decimal number at operators[0].rules[1]");
}
