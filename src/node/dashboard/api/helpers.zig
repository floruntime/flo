//! Dashboard API Shared Helpers
//!
//! Reusable utilities for all API domain modules:
//! - BinaryReader: Safe little-endian binary wire format parser
//! - Query parameter parsing (generic over int types + string)
//! - Route-to-shard helper (hash-based routing)
//! - JSON error responses
//! - Stream discovery from state engines (used by streams, namespaces, groups)
//! - Discovered* types + free helpers for actions, workers, runs

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
pub const json = @import("../../../util/json.zig");
pub const routing = @import("../../dispatch/routing.zig");
pub const Dispatcher = @import("../../dispatch/dispatcher.zig").Dispatcher;
pub const Core = @import("../../core/core.zig").Core;
pub const MetricsRegistry = @import("../../../metrics/registry.zig").MetricsRegistry;
pub const protocol = @import("../../protocol/result.zig");
pub const Command = protocol.Command;
pub const StartOffset = protocol.StartOffset;
pub const CommandResult = @import("../../protocol/result.zig").CommandResult;
pub const Tier = @import("../../../engine/consensus/mod.zig").Tier;
pub const StateEngine = @import("../../../engine/state/mod.zig").StateEngine;
pub const ActionMeta = @import("../../../actions/types.zig").ActionMeta;
pub const ActionRun = @import("../../../actions/types.zig").ActionRun;
pub const WorkerMeta = @import("../../../actions/types.zig").WorkerMeta;
pub const action_keys = @import("../../../actions/keys.zig");

// =============================================================================
// BinaryReader — Safe little-endian binary wire format parser
// =============================================================================

/// Reads little-endian integers and byte slices from a binary buffer with
/// bounds checking. Returns `null` when buffer is exhausted.
pub const BinaryReader = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) BinaryReader {
        return .{ .data = data, .pos = 0 };
    }

    pub fn readU16(self: *BinaryReader) ?u16 {
        if (self.pos + 2 > self.data.len) return null;
        const v = std.mem.readInt(u16, self.data[self.pos..][0..2], .little);
        self.pos += 2;
        return v;
    }

    pub fn readU32(self: *BinaryReader) ?u32 {
        if (self.pos + 4 > self.data.len) return null;
        const v = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return v;
    }

    pub fn readU64(self: *BinaryReader) ?u64 {
        if (self.pos + 8 > self.data.len) return null;
        const v = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return v;
    }

    pub fn readI32(self: *BinaryReader) ?i32 {
        if (self.pos + 4 > self.data.len) return null;
        const v = std.mem.readInt(i32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return v;
    }

    pub fn readI64(self: *BinaryReader) ?i64 {
        if (self.pos + 8 > self.data.len) return null;
        const v = std.mem.readInt(i64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return v;
    }

    pub fn readByte(self: *BinaryReader) ?u8 {
        if (self.pos >= self.data.len) return null;
        const v = self.data[self.pos];
        self.pos += 1;
        return v;
    }

    pub fn readBytes(self: *BinaryReader, len: usize) ?[]const u8 {
        if (self.pos + len > self.data.len) return null;
        const slice = self.data[self.pos..][0..len];
        self.pos += len;
        return slice;
    }

    /// Read a length-prefixed byte slice. The length prefix type is compile-time selected.
    pub fn readLenPrefixed(self: *BinaryReader, comptime LenType: type) ?[]const u8 {
        const len: usize = switch (LenType) {
            u16 => @intCast(self.readU16() orelse return null),
            u32 => @intCast(self.readU32() orelse return null),
            else => @compileError("Unsupported length prefix type"),
        };
        return self.readBytes(len);
    }
};

// =============================================================================
// Query Parameter Parsing
// =============================================================================

/// Generic query string parameter parser — works with u32, u64, and []const u8.
pub fn parseQueryParam(comptime T: type, query_string: ?[]const u8, key: []const u8) ?T {
    const qs = query_string orelse return null;
    var pairs = std.mem.splitScalar(u8, qs, '&');
    while (pairs.next()) |pair| {
        if (std.mem.indexOfScalar(u8, pair, '=')) |eq_idx| {
            const k = pair[0..eq_idx];
            const v = pair[eq_idx + 1 ..];
            if (std.mem.eql(u8, k, key)) {
                if (T == []const u8) {
                    return if (v.len == 0) null else v;
                } else {
                    return std.fmt.parseInt(T, v, 10) catch null;
                }
            }
        }
    }
    return null;
}

// =============================================================================
// Shard Routing
// =============================================================================

/// Hash-route a namespaced key to the owning dispatcher (shard).
/// Delegates to Dispatcher.shardForKey so the routing logic lives in one place.
pub fn routeToShard(dispatchers: []*Dispatcher, namespace: []const u8, key: []const u8) *Dispatcher {
    if (dispatchers.len == 0) return dispatchers[0];
    const owner = dispatchers[0].shardForKey(namespace, key);
    return if (owner < dispatchers.len) dispatchers[owner] else dispatchers[0];
}

// =============================================================================
// JSON Error Response
// =============================================================================

/// Create JSON error response: {"error":"<message>"}
pub fn jsonError(allocator: Allocator, message: []const u8) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);

    try std.fmt.format(json_buf.writer(allocator),
        \\{{"error":"{s}"}}
    , .{message});

    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Stream Discovery — Shared by streams, namespaces, and consumer group endpoints
// =============================================================================

/// Stream info discovered from state engine scan
pub const DiscoveredStream = struct {
    name: []const u8,
    namespace: []const u8,
    partition_count: u32,
};

/// Scan all cores' state engines for streams.
/// Key format in state engine: "ns:{namespace}:stream:{topic}:info"
/// Caller owns returned slice and must free via `freeDiscoveredStreams`.
pub fn scanStreamsFromCores(allocator: Allocator, cores: ?[]*Core, ns_filter: ?[]const u8) ![]DiscoveredStream {
    const c = cores orelse return try allocator.alloc(DiscoveredStream, 0);
    if (c.len == 0) return try allocator.alloc(DiscoveredStream, 0);

    var streams: std.ArrayList(DiscoveredStream) = .empty;
    errdefer {
        for (streams.items) |s| {
            allocator.free(s.name);
            allocator.free(s.namespace);
        }
        streams.deinit(allocator);
    }

    for (c) |core| {
        const state = core.state_engine;

        var prefix_buf: [256]u8 = undefined;
        const prefix = if (ns_filter) |ns|
            std.fmt.bufPrint(&prefix_buf, "ns:{s}:stream:", .{ns}) catch continue
        else
            std.fmt.bufPrint(&prefix_buf, "ns:", .{}) catch continue;

        var iter = state.scan(prefix);

        while (iter.next()) |entry| {
            const key = entry.key;

            if (!std.mem.endsWith(u8, key, ":info")) continue;

            const ns_start = if (std.mem.startsWith(u8, key, "ns:")) @as(usize, 3) else continue;
            const stream_marker = std.mem.indexOf(u8, key[ns_start..], ":stream:") orelse continue;
            const namespace = key[ns_start .. ns_start + stream_marker];

            const after_stream = key[ns_start + stream_marker + 8 ..];
            const colon_idx = std.mem.lastIndexOf(u8, after_stream, ":") orelse continue;
            const topic = after_stream[0..colon_idx];

            if (topic.len == 0) continue;

            var found = false;
            for (streams.items) |s| {
                if (std.mem.eql(u8, s.name, topic) and std.mem.eql(u8, s.namespace, namespace)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                const name_copy = try allocator.dupe(u8, topic);
                errdefer allocator.free(name_copy);
                const ns_copy = try allocator.dupe(u8, namespace);
                errdefer allocator.free(ns_copy);
                try streams.append(allocator, .{
                    .name = name_copy,
                    .namespace = ns_copy,
                    .partition_count = 1,
                });
            }
        }
    }

    return try streams.toOwnedSlice(allocator);
}

/// Free a slice returned by `scanStreamsFromCores`.
pub fn freeDiscoveredStreams(allocator: Allocator, discovered: []DiscoveredStream) void {
    for (discovered) |s| {
        allocator.free(s.name);
        allocator.free(s.namespace);
    }
    allocator.free(discovered);
}

/// Scan streams and find the namespace for a given stream name.
/// Returns the namespace (slice into discovered) and the discovered list.
/// Caller must call `freeDiscoveredStreams` on the returned `.discovered`.
pub fn discoverStreamNamespace(
    allocator: Allocator,
    cores: ?[]*Core,
    stream_name: []const u8,
) !struct { ns: []const u8, discovered: []DiscoveredStream } {
    const discovered = try scanStreamsFromCores(allocator, cores, null);
    var ns: []const u8 = "default";
    for (discovered) |s| {
        if (std.mem.eql(u8, s.name, stream_name)) {
            ns = s.namespace;
            break;
        }
    }
    return .{ .ns = ns, .discovered = discovered };
}

/// Count streams per namespace by scanning state engines
pub fn countStreamsInNamespace(cores: ?[]*Core, namespace: []const u8) u32 {
    const c = cores orelse return 0;
    if (c.len == 0) return 0;

    var count: u32 = 0;
    var seen_buf: [64][]const u8 = undefined;
    var seen_count: usize = 0;

    for (c) |core| {
        const state = core.state_engine;

        var prefix_buf: [256]u8 = undefined;
        const prefix = std.fmt.bufPrint(&prefix_buf, "ns:{s}:stream:", .{namespace}) catch continue;
        var iter = state.scan(prefix);

        while (iter.next()) |entry| {
            const key = entry.key;
            if (!std.mem.endsWith(u8, key, ":info")) continue;

            const after_prefix = key[prefix.len..];
            const colon_idx = std.mem.lastIndexOf(u8, after_prefix, ":") orelse continue;
            const topic = after_prefix[0..colon_idx];
            if (topic.len == 0) continue;

            var found = false;
            for (seen_buf[0..seen_count]) |s| {
                if (std.mem.eql(u8, s, topic)) {
                    found = true;
                    break;
                }
            }
            if (!found and seen_count < seen_buf.len) {
                seen_buf[seen_count] = topic;
                seen_count += 1;
                count += 1;
            }
        }
    }
    return count;
}

// =============================================================================
// Action/Worker/Run Types + Free Helpers
// =============================================================================

/// Discovered action from state engine scan
pub const DiscoveredAction = struct {
    name: []const u8,
    namespace: []const u8,
    action_type: []const u8,
    owner: []const u8,
    description: []const u8,
    version: []const u8,
    enabled: bool,
    timeout_ms: u32,
    max_retries: u32,
    retry_delay_ms: u32,
    trigger_stream: []const u8,
    trigger_group: []const u8,
    created_at: i64,
    updated_at: i64,
};

pub fn freeDiscoveredActions(allocator: Allocator, discovered: []DiscoveredAction) void {
    for (discovered) |a| {
        allocator.free(a.name);
        allocator.free(a.namespace);
        allocator.free(a.action_type);
        allocator.free(a.owner);
        allocator.free(a.description);
        allocator.free(a.version);
        allocator.free(a.trigger_stream);
        allocator.free(a.trigger_group);
    }
    allocator.free(discovered);
}

/// Discovered worker from state engine scan
pub const DiscoveredWorker = struct {
    worker_id: []const u8,
    namespace: []const u8,
    task_types: []const u8,
    healthy: bool,
    current_load: u8,
    max_concurrent: u32,
    active_tasks: u32,
    last_seen: i64,
    registered_at: i64,
};

pub fn freeDiscoveredWorkers(allocator: Allocator, discovered: []DiscoveredWorker) void {
    for (discovered) |w| {
        allocator.free(w.worker_id);
        allocator.free(w.namespace);
        allocator.free(w.task_types);
    }
    allocator.free(discovered);
}

/// Run statistics counters
pub const RunCounts = struct {
    total: u32 = 0,
    pending: u32 = 0,
    running: u32 = 0,
    completed: u32 = 0,
    failed: u32 = 0,
    cancelled: u32 = 0,
    timed_out: u32 = 0,
};

/// Discovered run from state engine scan
pub const DiscoveredRun = struct {
    run_id: []const u8,
    status: []const u8,
    attempt: u32,
    created_at: i64,
    started_at: i64,
    completed_at: i64,
    worker_id: []const u8,
    error_message: []const u8,
    outcome: []const u8,
};

pub fn freeDiscoveredRuns(allocator: Allocator, runs: []DiscoveredRun) void {
    for (runs) |r| {
        allocator.free(r.run_id);
        allocator.free(r.status);
        if (r.worker_id.len > 0) allocator.free(r.worker_id);
        if (r.error_message.len > 0) allocator.free(r.error_message);
        if (r.outcome.len > 0) allocator.free(r.outcome);
    }
    allocator.free(runs);
}

/// Write RunCounts as a nested JSON object field
pub fn writeRunCountsJson(obj: anytype, counts: RunCounts) !void {
    var runs_obj = try obj.objectField("runs");
    try runs_obj.begin();
    try runs_obj.intField("total", counts.total);
    try runs_obj.intField("pending", counts.pending);
    try runs_obj.intField("running", counts.running);
    try runs_obj.intField("completed", counts.completed);
    try runs_obj.intField("failed", counts.failed);
    try runs_obj.intField("cancelled", counts.cancelled);
    try runs_obj.intField("timed_out", counts.timed_out);
    try runs_obj.end();
}

/// Write a DiscoveredRun as JSON fields into an object builder
pub fn writeRunJson(obj: anytype, run: DiscoveredRun) !void {
    try obj.stringField("run_id", run.run_id);
    try obj.stringField("status", run.status);
    try obj.intField("attempt", run.attempt);
    try obj.intField("created_at", run.created_at);
    if (run.started_at != 0) try obj.intField("started_at", run.started_at);
    if (run.completed_at != 0) try obj.intField("completed_at", run.completed_at);
    if (run.worker_id.len > 0) try obj.stringField("worker_id", run.worker_id);
    if (run.error_message.len > 0) try obj.stringField("error", run.error_message);
    if (run.outcome.len > 0) try obj.stringField("outcome", run.outcome);
}
