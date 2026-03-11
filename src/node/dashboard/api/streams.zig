//! Dashboard API — Stream & Consumer Group Endpoints
//!
//! - GET /streams                                     — All streams
//! - GET /streams/:name                               — Stream detail with partitions & groups
//! - GET /streams/:name/messages                      — Paginated messages from stream
//! - GET /streams/:name/groups/:group                 — Consumer group detail
//! - GET /streams/:name/groups/:group/members         — Consumer group members
//! - GET /streams/:name/groups/:group/pending         — Pending messages

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Shard = @import("../../shard.zig").Shard;
const StreamProjection = @import("../../../projection/stream.zig").StreamProjection;

// ── Helpers ─────────────────────────────────────────────────────────

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

fn getStreamProjection(ctx: *DashboardContext, idx: usize) ?*StreamProjection {
    const shard = getShard(ctx, idx) orelse return null;
    return &shard.defaultPartition().stream;
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// GET /streams - List all streams
pub fn getStreams(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Read from MetricsRegistry when populated
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        // Collect unique stream names from metrics
        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();

        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            const stream_entry = entry.value_ptr.*;
            const gop = try seen.getOrPut(stream_entry.topic);
            if (!gop.found_existing) {
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", stream_entry.topic);
                try obj.stringField("namespace", stream_entry.namespace);
                try obj.intField("partitions", 1);
                try obj.intField("ingest_rate", 0);
                try obj.intField("reads", 0);
                try obj.stringField("retention", "7d");
                try obj.end();
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name - Stream detail with partitions and consumer groups
pub fn getStreamDetail(allocator: Allocator, stream_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", stream_name);

    // Namespace + stats from MetricsRegistry (thread-safe)
    var ns: []const u8 = "default";
    var total_appends: u64 = 0;
    var total_bytes: u64 = 0;
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();
        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.topic, stream_name)) {
                ns = entry.value_ptr.namespace;
                const snap = entry.value_ptr.metrics.snapshot();
                total_appends += snap.append_records_total;
                total_bytes += snap.append_bytes_total;
            }
        }
    }

    try obj.stringField("namespace", ns);
    try obj.intField("total_count", total_appends);
    try obj.intField("total_bytes", total_bytes);

    // Partitions — read record count from shard 0's stream projection
    {
        var parts_arr = try obj.arrayField("partitions");
        try parts_arr.begin();
        if (getStreamProjection(ctx, 0)) |sp| {
            const name_hash = std.hash.Wyhash.hash(0, stream_name);
            const record_count = sp.streamRecordCount(name_hash);
            if (record_count > 0 or sp.streamCount() > 0) {
                try parts_arr.next();
                var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try pobj.begin();
                try pobj.intField("id", 0);
                try pobj.intField("record_count", record_count);
                try pobj.intField("stored_bytes", total_bytes);
                try pobj.end();
            }
        }
        try parts_arr.end();
    }

    // Consumer groups — aggregated from all shard projections
    {
        var groups_arr = try obj.arrayField("consumer_groups");
        try groups_arr.begin();
        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getStreamProjection(ctx, i)) |sp| {
                var git = sp.groups.iterator();
                while (git.next()) |ge| {
                    try groups_arr.next();
                    var gobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try gobj.begin();
                    try gobj.stringField("name", ge.key_ptr.*);
                    try gobj.intField("members", ge.value_ptr.members.count());
                    try gobj.intField("pending_count", ge.value_ptr.pelCount());
                    try gobj.end();
                }
            }
        }
        try groups_arr.end();
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/messages - Paginated messages
/// Query params: ?cursor=<ts>-<seq>&limit=&partition=
/// cursor format: "<timestamp_ms>-<sequence>" — omit for start of stream
pub fn getStreamMessages(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 2000;

    // Parse cursor "ts-seq" into StreamID
    const StreamID = @import("../../../projection/stream.zig").StreamID;
    const cursor_str = h.parseQueryParam([]const u8, query_string, "cursor");
    const start_id = if (cursor_str) |cs| blk: {
        // Find the '-' separator
        const sep_pos = std.mem.indexOfScalar(u8, cs, '-') orelse break :blk StreamID.MIN;
        const ts = std.fmt.parseInt(u64, cs[0..sep_pos], 10) catch break :blk StreamID.MIN;
        const seq = std.fmt.parseInt(u64, cs[sep_pos + 1 ..], 10) catch break :blk StreamID.MIN;
        break :blk StreamID{ .timestamp_ms = ts, .sequence = seq };
    } else StreamID.MIN;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var msg_count: u64 = 0;
    var total_count: u64 = 0;
    var last_id: StreamID = StreamID.MIN;

    {
        var msgs_arr = try obj.arrayField("messages");
        try msgs_arr.begin();

        // Read from shard 0's stream projection using StreamRecord
        if (getStreamProjection(ctx, 0)) |sp| {
            const name_hash = std.hash.Wyhash.hash(0, stream_name);
            total_count = sp.streamRecordCount(name_hash);
            const cap: usize = @min(@as(usize, @intCast(limit)), 1000);

            const StreamRecord = @import("../../../projection/stream.zig").StreamRecord;
            const buf = allocator.alloc(StreamRecord, cap) catch null;
            defer if (buf) |b| allocator.free(b);
            if (buf) |b| {
                const read_count = sp.readStreamAfter(name_hash, start_id, null, b);
                for (b[0..read_count]) |rec| {
                    try msgs_arr.next();
                    var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try mobj.begin();
                    try mobj.intField("id_ms", @as(i64, @intCast(rec.id.timestamp_ms)));
                    try mobj.intField("id_seq", @as(i64, @intCast(rec.id.sequence)));
                    try mobj.intField("ual_index", @as(i64, @intCast(rec.ual_index)));
                    try mobj.intField("size", 0);
                    try mobj.end();
                    last_id = rec.id;
                    msg_count += 1;
                }
            }
        }

        try msgs_arr.end();
    }

    // Next cursor — only present if we returned records
    if (msg_count > 0) {
        // Format as "ts-seq"
        var cursor_buf: [64]u8 = undefined;
        const cursor_slice = std.fmt.bufPrint(&cursor_buf, "{d}-{d}", .{ last_id.timestamp_ms, last_id.sequence }) catch "";
        try obj.stringField("next_cursor", cursor_slice);
    }

    try obj.intField("limit", @as(i64, @intCast(limit)));
    try obj.intField("count", msg_count);
    try obj.intField("total_count", total_count);
    try obj.intField("total_bytes", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group - Consumer group detail
pub fn getGroupDetail(allocator: Allocator, stream_name: []const u8, group_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    try obj.stringField("namespace", "default");

    // Find group across shard projections
    var found = false;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            if (sp.getGroup(group_name)) |group| {
                found = true;
                try obj.intField("generation", 0);
                try obj.intField("partition_count", 1);
                try obj.intField("member_count", group.members.count());

                {
                    var members_arr = try obj.arrayField("members");
                    try members_arr.begin();
                    var mit = group.members.iterator();
                    while (mit.next()) |me| {
                        try members_arr.next();
                        var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try mobj.begin();
                        try mobj.stringField("id", me.value_ptr.id);
                        try mobj.stringField("state", if (me.value_ptr.state == .active) "active" else "leaving");
                        try mobj.end();
                    }
                    try members_arr.end();
                }

                {
                    var assignments_arr = try obj.arrayField("assignments");
                    try assignments_arr.begin();
                    try assignments_arr.end();
                }

                try obj.intField("pending_count", group.pelCount());
                break;
            }
        }
    }

    if (!found) {
        try obj.intField("generation", 0);
        try obj.intField("partition_count", 1);
        try obj.intField("member_count", 0);
        var members_arr = try obj.arrayField("members");
        try members_arr.begin();
        try members_arr.end();
        var assignments_arr = try obj.arrayField("assignments");
        try assignments_arr.begin();
        try assignments_arr.end();
        try obj.intField("pending_count", 0);
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group/members - Consumer group members (flat array)
pub fn getGroupMembers(allocator: Allocator, stream_name: []const u8, group_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = stream_name;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            if (sp.getGroup(group_name)) |group| {
                var mit = group.members.iterator();
                while (mit.next()) |me| {
                    try arr.next();
                    var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try mobj.begin();
                    try mobj.stringField("id", me.value_ptr.id);
                    try mobj.stringField("state", if (me.value_ptr.state == .active) "active" else "leaving");
                    try mobj.intField("joined_at", @as(i64, @intCast(me.value_ptr.joined_at_ns / std.time.ns_per_ms)));
                    try mobj.end();
                }
                break;
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group/pending - Pending messages
pub fn getGroupPending(allocator: Allocator, stream_name: []const u8, group_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = stream_name;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    const PendingEntry = @import("../../../projection/stream.zig").PendingEntry;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    var pending_arr = try obj.arrayField("pending");
    try pending_arr.begin();

    var pel_count: usize = 0;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            if (sp.getGroup(group_name)) |group| {
                pel_count = group.pelCount();
                const cap: usize = @min(pel_count, 1000);
                const buf = allocator.alloc(PendingEntry, cap) catch null;
                defer if (buf) |b| allocator.free(b);
                if (buf) |b| {
                    const count = group.getPending(null, b);
                    for (b[0..count]) |pe| {
                        try pending_arr.next();
                        var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try pobj.begin();
                        try pobj.intField("id_ms", @as(i64, @intCast(pe.id.timestamp_ms)));
                        try pobj.intField("id_seq", @as(i64, @intCast(pe.id.sequence)));
                        try pobj.stringField("consumer", pe.consumer);
                        try pobj.intField("delivered_at_ms", @as(i64, @intCast(pe.delivered_at_ms)));
                        try pobj.intField("delivery_count", @as(i64, @intCast(pe.delivery_count)));
                        try pobj.end();
                    }
                }
                break;
            }
        }
    }

    try pending_arr.end();
    try obj.intField("count", pel_count);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Tests
// =============================================================================

test "getStreams returns empty when no streams registered" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getStreams(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "getStreamDetail returns detail without shards" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getStreamDetail(allocator, "events", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"events\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"partitions\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"consumer_groups\":[]") != null);
}

test "getStreamMessages returns empty" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getStreamMessages(allocator, "events", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"messages\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"count\":0") != null);
}
