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
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for stream info
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", stream_name);
    try obj.stringField("namespace", "default");
    try obj.intField("total_count", 0);
    try obj.intField("total_bytes", 0);

    var parts_arr = try obj.arrayField("partitions");
    try parts_arr.begin();
    try parts_arr.end();

    var groups_arr = try obj.arrayField("consumer_groups");
    try groups_arr.begin();
    try groups_arr.end();

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/messages - Paginated messages
/// Query params: ?offset=&limit=
pub fn getStreamMessages(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam(u64, query_string, "offset");
    _ = h.parseQueryParam(u32, query_string, "limit");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for stream read
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    var msgs_arr = try obj.arrayField("messages");
    try msgs_arr.begin();
    try msgs_arr.end();
    try obj.intField("count", 0);
    try obj.boolField("has_more", false);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group - Consumer group detail
pub fn getGroupDetail(allocator: Allocator, stream_name: []const u8, group_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for consumer group info
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    try obj.intField("members", 0);
    try obj.intField("lag", 0);
    try obj.stringField("status", "active");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group/members - Consumer group members
pub fn getGroupMembers(allocator: Allocator, stream_name: []const u8, group_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for group member list
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    var members_arr = try obj.arrayField("members");
    try members_arr.begin();
    try members_arr.end();
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group/pending - Pending messages
pub fn getGroupPending(allocator: Allocator, stream_name: []const u8, group_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for pending messages
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
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

test "getStreamDetail returns stub detail" {
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
