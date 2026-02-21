//! Processing Job Definition Parser
//!
//! Parses job definitions from YAML or JSON format, following the same
//! patterns as the workflow parser (`src/workflow/parser.zig`).
//!
//! # Supported Formats
//!
//! - YAML (primary, converted to JSON internally via util/yaml_to_json)
//! - JSON (native via std.json)
//! # Source/Sink Format
//!
//! Sources and sinks are specified as arrays (`sources:` / `sinks:`).
//!
//! Both stream and TS sources use the same nested object syntax:
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
//! # Usage
//!
//! ```zig
//! const parser = @import("processing/parser.zig");
//!
//! var def = try parser.parseJobDefinition(allocator, yaml_content);
//! defer def.deinit(allocator);
//! ```
//!
//! Only nested YAML is accepted on the server side. Flat dotted-key format
//! (e.g., `source.stream: x`) is a test convenience — the CLI test helper
//! `writeDottedToTempYaml` converts it to proper nested YAML before sending.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const job_definition = @import("definition.zig");
const yaml_to_json = @import("../util/yaml_to_json.zig");

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
// JSON Value Helpers (mirroring workflow/parser.zig)
// =============================================================================

const JsonValue = std.json.Value;
const JsonObjectMap = std.json.ObjectMap;

fn getString(obj: JsonValue, key: []const u8) ?[]const u8 {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return if (val == .string) val.string else null;
}

fn getInt(obj: JsonValue, key: []const u8) ?i64 {
    if (obj != .object) return null;
    const val = obj.object.get(key) orelse return null;
    return switch (val) {
        .integer => val.integer,
        .string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
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
// Job Definition Parser
// =============================================================================

/// Parse a job definition from YAML or JSON content.
///
/// Tries JSON first, falls back to YAML→JSON conversion.
/// Requires the plural `sources:`/`sinks:` array format.
///
/// ```yaml
/// kind: Processing
/// name: my-pipeline
/// sources:
///   - name: events-source
///     stream:
///       name: events
/// sinks:
///   - name: output
///     stream:
///       name: results
/// ```
///
/// Caller owns the returned definition and must call `deinit()`.
pub fn parseJobDefinition(allocator: Allocator, content: []const u8) ParseError!JobDefinition {
    return parseJobDefinitionWithNamespace(allocator, content, null);
}

/// Parse a job definition with an optional fallback namespace.
///
/// Resolution order for source/sink namespaces:
///   1. Explicit `namespace:` on the individual source/sink
///   2. Top-level `namespace:` in the YAML definition
///   3. `fallback_namespace` (typically the command/job namespace)
///   4. `"default"`
pub fn parseJobDefinitionWithNamespace(allocator: Allocator, content: []const u8, fallback_namespace: ?[]const u8) ParseError!JobDefinition {
    // First, try to parse as JSON directly
    if (std.json.parseFromSlice(JsonValue, allocator, content, .{})) |parsed| {
        defer parsed.deinit();
        return parseJobDefinitionFromJson(allocator, parsed.value, fallback_namespace);
    } else |_| {
        // JSON parse failed — try converting from YAML
        const json_content = yaml_to_json.convert(allocator, content) catch {
            return ParseError.InvalidFormat;
        };
        defer allocator.free(json_content);

        const parsed = std.json.parseFromSlice(JsonValue, allocator, json_content, .{}) catch {
            return ParseError.InvalidFormat;
        };
        defer parsed.deinit();

        return parseJobDefinitionFromJson(allocator, parsed.value, fallback_namespace);
    }
}

/// Parse job definition from a parsed JSON value tree.
fn parseJobDefinitionFromJson(allocator: Allocator, root: JsonValue, fallback_namespace: ?[]const u8) ParseError!JobDefinition {
    if (root != .object) return ParseError.InvalidFormat;

    // Validate kind
    const kind = getString(root, "kind") orelse return ParseError.MissingRequiredField;
    if (!mem.eql(u8, kind, "Processing")) return ParseError.InvalidKind;

    // Accumulators with errdefer cleanup
    var name: ?[]u8 = null;
    var namespace: ?[]u8 = null;
    var parallelism: u32 = 1;
    var batch_size: u32 = 100;
    var sources: std.ArrayList(SourceSpec) = .empty;
    var sinks: std.ArrayList(SinkSpec) = .empty;
    var operators: std.ArrayList(OperatorSpec) = .empty;

    errdefer {
        if (name) |n| allocator.free(n);
        if (namespace) |ns| allocator.free(ns);
        freeSourceSpecs(allocator, &sources);
        freeSinkSpecs(allocator, &sinks);
        freeOperatorSpecs(allocator, &operators);
    }

    // --- name (optional, defaults to "unnamed-job") ---
    if (getString(root, "name")) |v| {
        name = allocator.dupe(u8, v) catch return error.OutOfMemory;
    }

    // --- namespace (optional, defaults to fallback_namespace or "default") ---
    //
    // Resolution order for sources/sinks:
    //   1. Explicit namespace on the individual source/sink
    //   2. Top-level namespace in the YAML definition
    //   3. fallback_namespace (command/job-level namespace)
    //   4. "default"
    const effective_namespace: []const u8 = getString(root, "namespace") orelse
        (fallback_namespace orelse "default");

    namespace = allocator.dupe(u8, effective_namespace) catch return error.OutOfMemory;

    // --- parallelism (top-level integer, required to be valid if present) ---
    if (root.object.get("parallelism")) |pval| {
        const v = switch (pval) {
            .integer => |i| i,
            .string => |s| std.fmt.parseInt(i64, s, 10) catch return error.InvalidParallelism,
            else => return error.InvalidParallelism,
        };
        if (v <= 0) return error.InvalidParallelism;
        parallelism = @intCast(v);
    }

    // --- batch_size (top-level integer default for all sources) ---
    if (root.object.get("batch_size")) |bval| {
        const v = switch (bval) {
            .integer => |i| i,
            .string => |s| std.fmt.parseInt(i64, s, 10) catch return error.InvalidFormat,
            else => return error.InvalidFormat,
        };
        if (v <= 0) return error.InvalidFormat;
        batch_size = @intCast(v);
    }

    // --- sources (required array) ---
    if (getArray(root, "sources")) |arr| {
        for (arr) |item| {
            try parseOneSource(allocator, item, batch_size, effective_namespace, &sources);
        }
    }

    // --- sinks (required array) ---
    if (getArray(root, "sinks")) |arr| {
        for (arr) |item| {
            try parseOneSink(allocator, item, effective_namespace, &sinks);
        }
    }

    // --- operators (array of { type, name, module, config... } objects) ---
    if (getArray(root, "operators")) |arr| {
        for (arr) |item| {
            if (item != .object) continue;
            const op_type = getString(item, "type") orelse continue;
            const op_name = getString(item, "name") orelse op_type;

            const type_dup = allocator.dupe(u8, op_type) catch return error.OutOfMemory;
            errdefer allocator.free(type_dup);
            const name_dup = allocator.dupe(u8, op_name) catch return error.OutOfMemory;
            errdefer allocator.free(name_dup);

            const module: ?[]u8 = if (getString(item, "module")) |mp|
                (allocator.dupe(u8, mp) catch return error.OutOfMemory)
            else
                null;

            // Parse config entries — all keys except reserved ones (type, name, module)
            const config: ?[]const OperatorSpec.ConfigEntry = blk: {
                const obj_map = item.object;
                // Reserved keys that are not config
                const reserved = [_][]const u8{ "type", "name", "module" };
                // Count non-reserved string entries
                var config_count: usize = 0;
                var it = obj_map.iterator();
                while (it.next()) |entry| {
                    const k = entry.key_ptr.*;
                    var is_reserved = false;
                    for (reserved) |r| {
                        if (std.mem.eql(u8, k, r)) {
                            is_reserved = true;
                            break;
                        }
                    }
                    if (!is_reserved) config_count += 1;
                }

                if (config_count == 0) break :blk null;

                const entries = allocator.alloc(OperatorSpec.ConfigEntry, config_count) catch return error.OutOfMemory;
                var idx: usize = 0;
                var it2 = obj_map.iterator();
                while (it2.next()) |entry| {
                    const k = entry.key_ptr.*;
                    var is_reserved = false;
                    for (reserved) |r| {
                        if (std.mem.eql(u8, k, r)) {
                            is_reserved = true;
                            break;
                        }
                    }
                    if (is_reserved) continue;

                    // Convert value to string representation
                    const val_str: []const u8 = switch (entry.value_ptr.*) {
                        .string => |s| s,
                        .integer => |n| std.fmt.allocPrint(allocator, "{d}", .{n}) catch return error.OutOfMemory,
                        .float => |f| std.fmt.allocPrint(allocator, "{d}", .{f}) catch return error.OutOfMemory,
                        .bool => |b| if (b) "true" else "false",
                        else => continue,
                    };

                    const key_dup = allocator.dupe(u8, k) catch return error.OutOfMemory;
                    const val_dup = switch (entry.value_ptr.*) {
                        .string => allocator.dupe(u8, val_str) catch return error.OutOfMemory,
                        .integer, .float => val_str, // already allocated by allocPrint
                        else => allocator.dupe(u8, val_str) catch return error.OutOfMemory,
                    };

                    entries[idx] = .{ .key = key_dup, .value = val_dup };
                    idx += 1;
                }

                break :blk entries[0..idx];
            };

            operators.append(allocator, .{
                .type_name = type_dup,
                .name = name_dup,
                .module = module,
                .config = config,
            }) catch return error.OutOfMemory;
        }
    }

    // --- checkpointing (optional object with interval_ms) ---
    var checkpoint_interval_ms: ?u64 = null;
    if (getObject(root, "checkpointing")) |cp_obj| {
        if (getInt(cp_obj, "interval_ms")) |v| {
            if (v > 0) checkpoint_interval_ms = @intCast(@as(i64, v));
        }
    }

    // Validate required fields
    if (sources.items.len == 0) return error.MissingSource;
    if (sinks.items.len == 0) return error.MissingSink;

    // Apply defaults
    if (name == null) {
        name = allocator.dupe(u8, "unnamed-job") catch return error.OutOfMemory;
    }

    return .{
        .name = name.?,
        .namespace = namespace.?,
        .parallelism = parallelism,
        .batch_size = batch_size,
        .sources = sources,
        .sinks = sinks,
        .operators = operators,
        .checkpoint_interval_ms = checkpoint_interval_ms,
    };
}

// =============================================================================
// Source / Sink Parsing Helpers
// =============================================================================

/// Parse a single source object and append to the sources list.
///
/// Source kind detection:
///   - `ts:` object present     → SourceKind.ts
///   - `stream:` object present → SourceKind.stream
///
/// Stream source syntax:
///   ```yaml
///   - name: events-source
///     stream:
///       name: input-events
///       namespace: production
///       partitions: all
///       batch_size: 100
///   ```
///
/// Partition syntax (stream sources):
///   - `partitions: 2`         → single partition (integer)
///   - `partitions: "0-63"`     → range → creates 64 SourceSpec entries
///   - `partitions: "0,3,7"`    → list  → creates 3 SourceSpec entries
///   - `partitions: "all"`      → sentinel (PARTITION_ALL), handler resolves
///   - omitted                  → defaults to all partitions (PARTITION_ALL)
///
/// TS source syntax:
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
fn parseOneSource(allocator: Allocator, item: JsonValue, default_batch_size: u32, default_namespace: []const u8, sources: *std.ArrayList(SourceSpec)) ParseError!void {
    if (item != .object) return;

    const base_name = getString(item, "name") orelse "default-source";

    // Detect TS source
    if (getObject(item, "ts")) |ts_obj| {
        try appendTsSource(allocator, base_name, ts_obj, default_batch_size, default_namespace, sources);
        return;
    }

    // Detect stream source:
    //   stream:
    //     name: input-events
    //     namespace: production
    //     partitions: all
    //     batch_size: 100
    if (getObject(item, "stream")) |stream_obj| {
        try appendStreamSource(allocator, base_name, stream_obj, default_batch_size, default_namespace, sources);
        return;
    }

    return error.MissingSourceStream;
}

/// Append a TS source spec parsed from a `ts:` object.
///
/// ```yaml
/// - name: cpu-metrics
///   ts:
///     measurement: cpu
///     namespace: production
///     tags:
///       host: web-01
///     field: usage_idle
///     poll_interval_ms: 500
///     batch_size: 200
/// ```
fn appendTsSource(
    allocator: Allocator,
    source_name: []const u8,
    ts_obj: JsonValue,
    default_batch_size: u32,
    default_namespace: []const u8,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    const measurement_raw = getString(ts_obj, "measurement") orelse return error.MissingSourceStream;
    const ns_raw = getString(ts_obj, "namespace") orelse default_namespace;
    const field_raw = getString(ts_obj, "field") orelse "";

    var bs: u32 = default_batch_size;
    if (getInt(ts_obj, "batch_size")) |v| {
        if (v > 0) bs = @intCast(@as(i64, v));
    }

    var poll_interval_ms: u32 = 1000;
    if (getInt(ts_obj, "poll_interval_ms")) |v| {
        if (v > 0) poll_interval_ms = @intCast(@as(i64, v));
    }

    const name_d = allocator.dupe(u8, source_name) catch return error.OutOfMemory;
    errdefer allocator.free(name_d);
    const stream_d = allocator.dupe(u8, "") catch return error.OutOfMemory;
    errdefer allocator.free(stream_d);
    const ns_d = allocator.dupe(u8, ns_raw) catch return error.OutOfMemory;
    errdefer allocator.free(ns_d);
    const meas_d = allocator.dupe(u8, measurement_raw) catch return error.OutOfMemory;
    errdefer allocator.free(meas_d);
    const field_d = allocator.dupe(u8, field_raw) catch return error.OutOfMemory;
    errdefer allocator.free(field_d);

    // Parse tags: object mapping tag_key → tag_value (flat pairs)
    var tag_keys: [][]const u8 = &.{};
    if (getObject(ts_obj, "tags")) |tags_obj| {
        if (tags_obj == .object) {
            const count = tags_obj.object.count();
            if (count > 0) {
                const keys = allocator.alloc([]const u8, count * 2) catch return error.OutOfMemory;
                var idx: usize = 0;
                var it = tags_obj.object.iterator();
                while (it.next()) |entry| {
                    keys[idx] = allocator.dupe(u8, entry.key_ptr.*) catch return error.OutOfMemory;
                    idx += 1;
                    const val_str = if (entry.value_ptr.* == .string) entry.value_ptr.string else entry.key_ptr.*;
                    keys[idx] = allocator.dupe(u8, val_str) catch return error.OutOfMemory;
                    idx += 1;
                }
                tag_keys = keys;
            }
        }
    }

    sources.append(allocator, .{
        .kind = .ts,
        .name = name_d,
        .stream = stream_d,
        .namespace = ns_d,
        .partition = 0,
        .batch_size = bs,
        .ts_measurement = meas_d,
        .ts_tags = tag_keys,
        .ts_field = field_d,
        .ts_poll_interval_ms = poll_interval_ms,
    }) catch return error.OutOfMemory;
}

/// Append a stream source spec parsed from a `stream:` object.
///
/// ```yaml
/// - name: events-source
///   stream:
///     name: input-events
///     namespace: production
///     partitions: all
///     batch_size: 100
/// ```
fn appendStreamSource(
    allocator: Allocator,
    source_name: []const u8,
    stream_obj: JsonValue,
    default_batch_size: u32,
    default_namespace: []const u8,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    const stream_name = getString(stream_obj, "name") orelse return error.MissingSourceStream;
    const ns_raw = getString(stream_obj, "namespace") orelse default_namespace;

    var bs: u32 = default_batch_size;
    if (getInt(stream_obj, "batch_size")) |v| {
        if (v > 0) bs = @intCast(@as(i64, v));
    }

    // `partitions:` as string → range/list/all expansion
    if (getString(stream_obj, "partitions")) |partitions_str| {
        try expandPartitions(allocator, source_name, stream_name, ns_raw, bs, partitions_str, sources);
        return;
    }

    // `partitions:` as integer → single partition; default → all
    var partition: u32 = job_definition.PARTITION_ALL;
    if (getInt(stream_obj, "partitions")) |v| {
        partition = @intCast(@as(i64, v));
    }

    const name_dup = allocator.dupe(u8, source_name) catch return error.OutOfMemory;
    errdefer allocator.free(name_dup);
    const stream_dup = allocator.dupe(u8, stream_name) catch return error.OutOfMemory;
    errdefer allocator.free(stream_dup);
    const ns_dup = allocator.dupe(u8, ns_raw) catch return error.OutOfMemory;

    sources.append(allocator, .{
        .name = name_dup,
        .stream = stream_dup,
        .namespace = ns_dup,
        .partition = partition,
        .batch_size = bs,
    }) catch return error.OutOfMemory;
}

/// Expand a `partitions:` string into multiple SourceSpec entries.
///
/// Supported formats:
///   - `"all"`   → single entry with partition = PARTITION_ALL (sentinel)
///   - `"0-63"`  → range from 0 to 63 inclusive (64 entries)
///   - `"0,3,7"` → comma-separated list (3 entries)
fn expandPartitions(
    allocator: Allocator,
    base_name: []const u8,
    stream_name: []const u8,
    ns_raw: []const u8,
    bs: u32,
    partitions_str: []const u8,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    const PARTITION_ALL = job_definition.PARTITION_ALL;

    if (std.mem.eql(u8, partitions_str, "all")) {
        // Sentinel — handler resolves actual count at runtime
        const name_d = allocator.dupe(u8, base_name) catch return error.OutOfMemory;
        errdefer allocator.free(name_d);
        const stream_d = allocator.dupe(u8, stream_name) catch return error.OutOfMemory;
        errdefer allocator.free(stream_d);
        const ns_d = allocator.dupe(u8, ns_raw) catch return error.OutOfMemory;

        sources.append(allocator, .{
            .name = name_d,
            .stream = stream_d,
            .namespace = ns_d,
            .partition = PARTITION_ALL,
            .batch_size = bs,
        }) catch return error.OutOfMemory;
        return;
    }

    // Try range format: "start-end"
    if (std.mem.indexOfScalar(u8, partitions_str, '-')) |dash_pos| {
        const start_str = partitions_str[0..dash_pos];
        const end_str = partitions_str[dash_pos + 1 ..];
        const start = std.fmt.parseInt(u32, start_str, 10) catch return error.InvalidPartitions;
        const end = std.fmt.parseInt(u32, end_str, 10) catch return error.InvalidPartitions;
        if (start > end) return error.InvalidPartitions;

        var p = start;
        while (p <= end) : (p += 1) {
            try appendExpandedSource(allocator, base_name, stream_name, ns_raw, p, bs, sources);
        }
        return;
    }

    // Comma-separated list: "0,3,7"
    var iter = std.mem.splitScalar(u8, partitions_str, ',');
    var count: u32 = 0;
    while (iter.next()) |seg| {
        const trimmed = std.mem.trim(u8, seg, " ");
        if (trimmed.len == 0) continue;
        const p = std.fmt.parseInt(u32, trimmed, 10) catch return error.InvalidPartitions;
        try appendExpandedSource(allocator, base_name, stream_name, ns_raw, p, bs, sources);
        count += 1;
    }
    if (count == 0) return error.InvalidPartitions;
}

/// Append a single expanded SourceSpec with a partition-suffixed name.
fn appendExpandedSource(
    allocator: Allocator,
    base_name: []const u8,
    stream_name: []const u8,
    ns_raw: []const u8,
    partition: u32,
    bs: u32,
    sources: *std.ArrayList(SourceSpec),
) ParseError!void {
    // Generate name: "base_name-p3"
    const name_d = std.fmt.allocPrint(allocator, "{s}-p{d}", .{ base_name, partition }) catch return error.OutOfMemory;
    errdefer allocator.free(name_d);
    const stream_d = allocator.dupe(u8, stream_name) catch return error.OutOfMemory;
    errdefer allocator.free(stream_d);
    const ns_d = allocator.dupe(u8, ns_raw) catch return error.OutOfMemory;

    sources.append(allocator, .{
        .name = name_d,
        .stream = stream_d,
        .namespace = ns_d,
        .partition = partition,
        .batch_size = bs,
    }) catch return error.OutOfMemory;
}

/// Parse a single sink object and append to the sinks list.
///
/// Sink kind detection:
///   - `kv:` object present  → SinkKind.kv
///   - `queue:` object present → SinkKind.queue
///   - `ts:` object present  → SinkKind.ts
///   - `stream:` object present → SinkKind.stream
///
/// Optional `routes_from:` field maps this sink to a named side output tag.
fn parseOneSink(allocator: Allocator, item: JsonValue, default_namespace: []const u8, sinks: *std.ArrayList(SinkSpec)) ParseError!void {
    if (item != .object) return;

    const sink_name = getString(item, "name") orelse "default-sink";
    const routes_from = getString(item, "routes_from");

    // Detect sink kind
    if (getObject(item, "kv")) |kv_obj| {
        try appendKvSink(allocator, sink_name, kv_obj, routes_from, default_namespace, sinks);
    } else if (getObject(item, "queue")) |q_obj| {
        try appendQueueSink(allocator, sink_name, q_obj, routes_from, default_namespace, sinks);
    } else if (getObject(item, "ts")) |ts_obj| {
        try appendTsSink(allocator, sink_name, ts_obj, routes_from, default_namespace, sinks);
    } else if (getObject(item, "stream")) |stream_obj| {
        try appendStreamSink(allocator, sink_name, stream_obj, routes_from, default_namespace, sinks);
    } else {
        return error.MissingSinkTarget;
    }
}

fn appendStreamSink(
    allocator: Allocator,
    sink_name: []const u8,
    stream_obj: JsonValue,
    routes_from: ?[]const u8,
    default_namespace: []const u8,
    sinks: *std.ArrayList(SinkSpec),
) ParseError!void {
    const stream_name = getString(stream_obj, "name") orelse return error.MissingSinkTarget;
    const ns_raw = getString(stream_obj, "namespace") orelse default_namespace;

    const name_d = allocator.dupe(u8, sink_name) catch return error.OutOfMemory;
    errdefer allocator.free(name_d);
    const target_d = allocator.dupe(u8, stream_name) catch return error.OutOfMemory;
    errdefer allocator.free(target_d);
    const ns_d = allocator.dupe(u8, ns_raw) catch return error.OutOfMemory;
    errdefer allocator.free(ns_d);
    const kp_d = allocator.dupe(u8, "") catch return error.OutOfMemory;
    errdefer allocator.free(kp_d);
    const sep_d = allocator.dupe(u8, ":") catch return error.OutOfMemory;
    errdefer allocator.free(sep_d);
    const wm_d = allocator.dupe(u8, "upsert") catch return error.OutOfMemory;
    errdefer allocator.free(wm_d);
    const rf_d: ?[]const u8 = if (routes_from) |rf| allocator.dupe(u8, rf) catch return error.OutOfMemory else null;

    sinks.append(allocator, .{
        .name = name_d,
        .kind = .stream,
        .target = target_d,
        .namespace = ns_d,
        .routes_from = rf_d,
        .key_prefix = kp_d,
        .separator = sep_d,
        .write_mode = wm_d,
        .ttl_ms = null,
        .priority = 0,
        .delay_ms = null,
        .use_key_as_dedup = true,
    }) catch return error.OutOfMemory;
}

fn appendKvSink(
    allocator: Allocator,
    sink_name: []const u8,
    kv_obj: JsonValue,
    routes_from: ?[]const u8,
    default_namespace: []const u8,
    sinks: *std.ArrayList(SinkSpec),
) ParseError!void {
    const name_d = allocator.dupe(u8, sink_name) catch return error.OutOfMemory;
    errdefer allocator.free(name_d);
    const target_d = allocator.dupe(u8, "") catch return error.OutOfMemory;
    errdefer allocator.free(target_d);
    const ns_d = allocator.dupe(u8, getString(kv_obj, "namespace") orelse default_namespace) catch return error.OutOfMemory;
    errdefer allocator.free(ns_d);
    const kp_d = allocator.dupe(u8, getString(kv_obj, "key_prefix") orelse "") catch return error.OutOfMemory;
    errdefer allocator.free(kp_d);
    const sep_d = allocator.dupe(u8, getString(kv_obj, "separator") orelse ":") catch return error.OutOfMemory;
    errdefer allocator.free(sep_d);
    const wm_d = allocator.dupe(u8, getString(kv_obj, "write_mode") orelse "upsert") catch return error.OutOfMemory;
    errdefer allocator.free(wm_d);
    const rf_d: ?[]const u8 = if (routes_from) |rf| allocator.dupe(u8, rf) catch return error.OutOfMemory else null;

    var ttl: ?u64 = null;
    if (getInt(kv_obj, "ttl_ms")) |v| {
        if (v > 0) ttl = @intCast(@as(i64, v));
    }

    sinks.append(allocator, .{
        .name = name_d,
        .kind = .kv,
        .target = target_d,
        .namespace = ns_d,
        .routes_from = rf_d,
        .key_prefix = kp_d,
        .separator = sep_d,
        .write_mode = wm_d,
        .ttl_ms = ttl,
        .priority = 0,
        .delay_ms = null,
        .use_key_as_dedup = true,
    }) catch return error.OutOfMemory;
}

fn appendQueueSink(
    allocator: Allocator,
    sink_name: []const u8,
    q_obj: JsonValue,
    routes_from: ?[]const u8,
    default_namespace: []const u8,
    sinks: *std.ArrayList(SinkSpec),
) ParseError!void {
    const name_d = allocator.dupe(u8, sink_name) catch return error.OutOfMemory;
    errdefer allocator.free(name_d);
    const target_d = allocator.dupe(u8, getString(q_obj, "name") orelse sink_name) catch return error.OutOfMemory;
    errdefer allocator.free(target_d);
    const ns_d = allocator.dupe(u8, getString(q_obj, "namespace") orelse default_namespace) catch return error.OutOfMemory;
    errdefer allocator.free(ns_d);
    const kp_d = allocator.dupe(u8, "") catch return error.OutOfMemory;
    errdefer allocator.free(kp_d);
    const sep_d = allocator.dupe(u8, ":") catch return error.OutOfMemory;
    errdefer allocator.free(sep_d);
    const wm_d = allocator.dupe(u8, "upsert") catch return error.OutOfMemory;
    errdefer allocator.free(wm_d);
    const rf_d: ?[]const u8 = if (routes_from) |rf| allocator.dupe(u8, rf) catch return error.OutOfMemory else null;

    var priority: u8 = 0;
    if (getInt(q_obj, "priority")) |v| {
        if (v >= 0 and v <= 255) priority = @intCast(@as(i64, v));
    }

    var delay: ?u64 = null;
    if (getInt(q_obj, "delay_ms")) |v| {
        if (v > 0) delay = @intCast(@as(i64, v));
    }

    var use_dedup = true;
    if (q_obj.object.get("use_key_as_dedup")) |dv| {
        if (dv == .bool) use_dedup = dv.bool;
    }

    sinks.append(allocator, .{
        .name = name_d,
        .kind = .queue,
        .target = target_d,
        .namespace = ns_d,
        .routes_from = rf_d,
        .key_prefix = kp_d,
        .separator = sep_d,
        .write_mode = wm_d,
        .ttl_ms = null,
        .priority = priority,
        .delay_ms = delay,
        .use_key_as_dedup = use_dedup,
    }) catch return error.OutOfMemory;
}

/// Append a time-series (ts) sink from YAML.
///
/// YAML format:
/// ```yaml
/// sinks:
///   - name: metrics-out
///     ts:
///       measurement: cpu_usage
///       namespace: monitoring
///       value_field: value       # shorthand: single numeric field (default)
///       tags:
///         host: hostname          # tag_key: json_key
///         region: dc
///       fields:
///         cpu: cpu_percent        # field_name: json_key
///         mem: mem_percent
/// ```
fn appendTsSink(
    allocator: Allocator,
    sink_name: []const u8,
    ts_obj: JsonValue,
    routes_from: ?[]const u8,
    default_namespace: []const u8,
    sinks: *std.ArrayList(SinkSpec),
) ParseError!void {
    const name_d = allocator.dupe(u8, sink_name) catch return error.OutOfMemory;
    errdefer allocator.free(name_d);

    const measurement_raw = getString(ts_obj, "measurement") orelse sink_name;
    const measurement_d = allocator.dupe(u8, measurement_raw) catch return error.OutOfMemory;
    errdefer allocator.free(measurement_d);

    const ns_d = allocator.dupe(u8, getString(ts_obj, "namespace") orelse default_namespace) catch return error.OutOfMemory;
    errdefer allocator.free(ns_d);

    // Default, unused fields for non-ts kinds
    const target_d = allocator.dupe(u8, measurement_raw) catch return error.OutOfMemory;
    errdefer allocator.free(target_d);
    const kp_d = allocator.dupe(u8, "") catch return error.OutOfMemory;
    errdefer allocator.free(kp_d);
    const sep_d = allocator.dupe(u8, ":") catch return error.OutOfMemory;
    errdefer allocator.free(sep_d);
    const wm_d = allocator.dupe(u8, "upsert") catch return error.OutOfMemory;
    errdefer allocator.free(wm_d);
    const rf_d: ?[]const u8 = if (routes_from) |rf| allocator.dupe(u8, rf) catch return error.OutOfMemory else null;

    // Parse value_field (shorthand for single field)
    const vf_raw = getString(ts_obj, "value_field") orelse "value";
    const vf_d = allocator.dupe(u8, vf_raw) catch return error.OutOfMemory;
    errdefer allocator.free(vf_d);

    // Parse tags: object mapping tag_key → json_key
    var tag_keys: [][]const u8 = &.{};
    if (getObject(ts_obj, "tags")) |tags_obj| {
        if (tags_obj == .object) {
            const count = tags_obj.object.count();
            if (count > 0) {
                const keys = allocator.alloc([]const u8, count * 2) catch return error.OutOfMemory;
                var idx: usize = 0;
                var it = tags_obj.object.iterator();
                while (it.next()) |entry| {
                    keys[idx] = allocator.dupe(u8, entry.key_ptr.*) catch return error.OutOfMemory;
                    idx += 1;
                    const val_str = if (entry.value_ptr.* == .string) entry.value_ptr.string else entry.key_ptr.*;
                    keys[idx] = allocator.dupe(u8, val_str) catch return error.OutOfMemory;
                    idx += 1;
                }
                tag_keys = keys;
            }
        }
    }

    // Parse fields: object mapping field_name → json_key
    var field_keys: [][]const u8 = &.{};
    if (getObject(ts_obj, "fields")) |fields_obj| {
        if (fields_obj == .object) {
            const count = fields_obj.object.count();
            if (count > 0) {
                const keys = allocator.alloc([]const u8, count * 2) catch return error.OutOfMemory;
                var idx: usize = 0;
                var it = fields_obj.object.iterator();
                while (it.next()) |entry| {
                    keys[idx] = allocator.dupe(u8, entry.key_ptr.*) catch return error.OutOfMemory;
                    idx += 1;
                    const val_str = if (entry.value_ptr.* == .string) entry.value_ptr.string else entry.key_ptr.*;
                    keys[idx] = allocator.dupe(u8, val_str) catch return error.OutOfMemory;
                    idx += 1;
                }
                field_keys = keys;
            }
        }
    }

    sinks.append(allocator, .{
        .name = name_d,
        .kind = .ts,
        .target = target_d,
        .namespace = ns_d,
        .routes_from = rf_d,
        .key_prefix = kp_d,
        .separator = sep_d,
        .write_mode = wm_d,
        .ttl_ms = null,
        .priority = 0,
        .delay_ms = null,
        .use_key_as_dedup = true,
        .ts_measurement = measurement_d,
        .ts_tag_keys = tag_keys,
        .ts_field_keys = field_keys,
        .ts_value_field = vf_d,
    }) catch return error.OutOfMemory;
}

// =============================================================================
// Cleanup Helpers (used by errdefer)
// =============================================================================

fn freeSourceSpecs(allocator: Allocator, sources: *std.ArrayList(SourceSpec)) void {
    for (sources.items) |src| {
        allocator.free(src.name);
        allocator.free(src.stream);
        allocator.free(src.namespace);
        // TS-specific fields
        if (src.ts_measurement.len > 0) allocator.free(src.ts_measurement);
        if (src.ts_field.len > 0) allocator.free(src.ts_field);
        for (src.ts_tags) |t| allocator.free(t);
        if (src.ts_tags.len > 0) allocator.free(src.ts_tags);
    }
    sources.deinit(allocator);
}

fn freeSinkSpecs(allocator: Allocator, sinks_list: *std.ArrayList(SinkSpec)) void {
    for (sinks_list.items) |snk| {
        allocator.free(snk.name);
        allocator.free(snk.target);
        allocator.free(snk.namespace);
        allocator.free(snk.key_prefix);
        allocator.free(snk.separator);
        allocator.free(snk.write_mode);
        if (snk.routes_from) |rf| allocator.free(rf);
    }
    sinks_list.deinit(allocator);
}

fn freeOperatorSpecs(allocator: Allocator, operators: *std.ArrayList(OperatorSpec)) void {
    for (operators.items) |op| {
        allocator.free(op.type_name);
        allocator.free(op.name);
        if (op.module) |mp| allocator.free(mp);
    }
    operators.deinit(allocator);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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
    try std.testing.expectError(error.MissingSourceStream, parseJobDefinition(allocator, text));
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
        \\      write_mode: upsert
        \\      ttl_ms: 86400000
        \\  - name: tasks
        \\    queue:
        \\      name: task-queue
        \\      namespace: work
        \\      priority: 5
        \\      delay_ms: 1000
        \\      use_key_as_dedup: false
    ;

    var def = try parseJobDefinition(allocator, text);
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
    try std.testing.expectEqual(@as(?u64, 1000), s2.delay_ms);
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
        \\  - type: map
        \\    name: transform
    ;

    var def = try parseJobDefinition(allocator, text);
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
    try std.testing.expectError(error.MissingSource, parseJobDefinition(allocator, text));
}

test "parser: missing sink" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: input
    ;
    try std.testing.expectError(error.MissingSink, parseJobDefinition(allocator, text));
}

test "parser: source without stream field" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - namespace: ns
        \\sinks:
        \\  - stream:
        \\      name: out
    ;
    try std.testing.expectError(error.MissingSourceStream, parseJobDefinition(allocator, text));
}

test "parser: sink without target" {
    const allocator = std.testing.allocator;

    const text =
        \\kind: Processing
        \\sources:
        \\  - stream:
        \\      name: in
        \\sinks:
        \\  - namespace: ns
    ;
    try std.testing.expectError(error.MissingSinkTarget, parseJobDefinition(allocator, text));
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
    try std.testing.expectError(error.InvalidParallelism, parseJobDefinition(allocator, text));
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
    try std.testing.expectError(error.InvalidParallelism, parseJobDefinition(allocator, text));
}

test "parser: empty text" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.MissingRequiredField, parseJobDefinition(allocator, ""));
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, json);
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

    var def = try parseJobDefinition(allocator, json);
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

    var def = try parseJobDefinition(allocator, text);
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
    try std.testing.expectError(error.MissingRequiredField, parseJobDefinition(allocator, text));
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
    try std.testing.expectError(error.InvalidKind, parseJobDefinition(allocator, text));
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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
    try std.testing.expectError(error.InvalidPartitions, parseJobDefinition(allocator, text));
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

    var def = try parseJobDefinition(allocator, text);
    defer def.deinit(allocator);

    // 1 single + 3 expanded = 4 total
    try std.testing.expectEqual(@as(usize, 4), def.sources.items.len);
    try std.testing.expectEqualStrings("single", def.sources.items[0].name);
    try std.testing.expectEqual(@as(u32, 7), def.sources.items[0].partition);
    try std.testing.expectEqualStrings("multi-p0", def.sources.items[1].name);
    try std.testing.expectEqual(@as(u32, 0), def.sources.items[1].partition);
}

// =============================================================================
// Side-Output Routing Tests
// =============================================================================

test "parser: sink with routes_from" {
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
        \\    routes_from: late
    ;

    var def = try parseJobDefinition(allocator, text);
    defer def.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), def.sinks.items.len);
    try std.testing.expect(def.sinks.items[0].routes_from == null);
    try std.testing.expectEqualStrings("late", def.sinks.items[1].routes_from.?);
}

test "parser: KV sink with routes_from" {
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
        \\    routes_from: error-records
    ;

    var def = try parseJobDefinition(allocator, text);
    defer def.deinit(allocator);

    const snk = def.primarySink().?;
    try std.testing.expect(snk.kind == .kv);
    try std.testing.expectEqualStrings("error-records", snk.routes_from.?);
}

test "parser: queue sink with routes_from" {
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
        \\    routes_from: failures
    ;

    var def = try parseJobDefinition(allocator, text);
    defer def.deinit(allocator);

    const snk = def.primarySink().?;
    try std.testing.expect(snk.kind == .queue);
    try std.testing.expectEqualStrings("failures", snk.routes_from.?);
    try std.testing.expectEqualStrings("dead-letter", snk.target);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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
    var def = try parseJobDefinitionWithNamespace(allocator, text, "my-team");
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
    var def = try parseJobDefinitionWithNamespace(allocator, text, "from-command");
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
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

    var def = try parseJobDefinition(allocator, text);
    defer def.deinit(allocator);

    try std.testing.expectEqualStrings("default", def.namespace);
    try std.testing.expectEqualStrings("default", def.primarySource().?.namespace);
    try std.testing.expectEqualStrings("default", def.primarySink().?.namespace);
}
