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

    // Partitions — read HWM from shard 0's stream projection
    {
        var parts_arr = try obj.arrayField("partitions");
        try parts_arr.begin();
        if (getStreamProjection(ctx, 0)) |sp| {
            const hwm = sp.highWaterMark();
            if (hwm > 0 or sp.streamCount() > 0) {
                try parts_arr.next();
                var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try pobj.begin();
                try pobj.intField("id", 0);
                try pobj.intField("high_water_mark", hwm);
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
                    try gobj.intField("committed_offset", ge.value_ptr.committed_offset);
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
/// Query params: ?offset=&limit=&partition=
pub fn getStreamMessages(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = stream_name;
    const offset = h.parseQueryParam(u64, query_string, "offset") orelse 0;
    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 2000;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var msg_count: u64 = 0;
    var total_count: u64 = 0;

    {
        var msgs_arr = try obj.arrayField("messages");
        try msgs_arr.begin();

        // Read from shard 0's stream projection
        if (getStreamProjection(ctx, 0)) |sp| {
            const hwm = sp.highWaterMark();
            total_count = hwm;
            const from = if (offset == 0) @as(u64, 1) else offset;
            const cap: usize = @min(@as(usize, @intCast(limit)), 1000);
            const to = @min(from +| @as(u64, @intCast(cap)), hwm + 1);

            if (from <= hwm) {
                const OffsetEntry = @import("../../../projection/stream.zig").OffsetEntry;
                const buf = allocator.alloc(OffsetEntry, cap) catch null;
                defer if (buf) |b| allocator.free(b);
                if (buf) |b| {
                    const count = sp.readRange(from, to, b);
                    for (b[0..count]) |oe| {
                        try msgs_arr.next();
                        var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try mobj.begin();
                        try mobj.intField("offset", @as(i64, @intCast(oe.ual_index)));
                        try mobj.intField("timestamp", @as(i64, @intCast(oe.timestamp_ns / std.time.ns_per_ms)));
                        try mobj.intField("size", 0);
                        try mobj.end();
                        msg_count += 1;
                    }
                }
            }
        }

        try msgs_arr.end();
    }

    try obj.intField("offset", @as(i64, @intCast(offset)));
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
                        try mobj.intField("committed_offset", me.value_ptr.committed_offset);
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

                try obj.intField("pending_count", 0);
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
                    try mobj.intField("committed_offset", me.value_ptr.committed_offset);
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
    _ = group_name;
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Pending tracking not yet implemented at projection level
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    var pending_arr = try obj.arrayField("pending");
    try pending_arr.begin();
    try pending_arr.end();
    try obj.intField("count", 0);
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
