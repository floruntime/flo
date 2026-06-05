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
const stream_mod = @import("../../../projection/stream.zig");
const StreamProjection = stream_mod.StreamProjection;
const ns_keys = @import("../../../namespace/handler.zig");
const router = @import("../../router.zig");

/// Unpack a stream-append batch blob into its individual record payloads, writing
/// payload slices (into `value`) to `out`. Batch wire format:
///   `[record_count:u32]( [payload_len:u32][payload][header_count:u16]([klen:u16][k][vlen:u16][v])* )*`
/// One append may carry N records; the dashboard must surface all of them (a
/// batch isn't a single record). Returns the number of payloads written.
fn unpackBatchPayloads(value: []const u8, out: [][]const u8) usize {
    if (value.len < 4) return 0;
    const rcount = std.mem.readInt(u32, value[0..4], .little);
    var pos: usize = 4;
    var n: usize = 0;
    var j: u32 = 0;
    while (j < rcount and n < out.len) : (j += 1) {
        if (pos + 4 > value.len) break;
        const plen = std.mem.readInt(u32, value[pos..][0..4], .little);
        pos += 4;
        if (pos + plen > value.len) break;
        out[n] = value[pos .. pos + plen];
        n += 1;
        pos += plen;
        // skip headers
        if (pos + 2 > value.len) break;
        const hcount = std.mem.readInt(u16, value[pos..][0..2], .little);
        pos += 2;
        var hh: u16 = 0;
        while (hh < hcount) : (hh += 1) {
            if (pos + 2 > value.len) return n;
            const klen = std.mem.readInt(u16, value[pos..][0..2], .little);
            pos += 2 + klen;
            if (pos + 2 > value.len) return n;
            const vlen = std.mem.readInt(u16, value[pos..][0..2], .little);
            pos += 2 + vlen;
        }
    }
    return n;
}

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

/// Logical record count for a stream = sum of each append batch's record count
/// (a batch may carry N records). `sp.streamRecordCount` only counts append
/// entries, so it under-reports for batched producers. Reads each batch's count
/// header via the zero-copy UAL read (cheap, no payload copy). Bounded by
/// `MAX_BATCHES`; beyond that, extrapolates from the sampled prefix.
fn streamLogicalCount(allocator: Allocator, partition: anytype, sp: *StreamProjection, name_hash: u64) u64 {
    const batches = sp.streamRecordCount(name_hash);
    if (batches == 0) return 0;
    const MAX_BATCHES: usize = 16384;
    const to_read = @min(batches, MAX_BATCHES);
    const buf = allocator.alloc(stream_mod.StreamRecord, to_read) catch return batches;
    defer allocator.free(buf);
    const n = sp.readStreamAfter(name_hash, stream_mod.StreamID.MIN, null, buf);
    if (n == 0) return batches;
    var sum: u64 = 0;
    for (buf[0..n]) |rec| {
        var c: u64 = 1;
        if (partition.ual.read(rec.ual_index)) |entry| {
            if (entry.commandPayload()) |cmd| {
                const batch = stream_mod.decodeAppendValue(cmd.value).payload;
                if (batch.len >= 4) {
                    const rc = std.mem.readInt(u32, batch[0..4], .little);
                    if (rc > 0) c = rc;
                }
            }
        }
        sum += c;
    }
    // Extrapolate if the stream has more batches than we sampled.
    if (batches > n) return sum * batches / n;
    return sum;
}

/// GET /streams - List all streams
pub fn getStreams(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // De-duplicate across shards
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    // Build namespace prefix for filtering (same logic as stream handler)
    var ns_prefix_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    const ns_prefix = if (ns_filter) |ns| ns_keys.namespacePrefix(&ns_prefix_buf, ns) else &[_]u8{};
    const effective_ns = ns_filter orelse "default";

    // Scan StreamProjection on each shard for registered stream names
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            var name_buf: [1024][]const u8 = undefined;
            const count = sp.scanStreamNames(&name_buf);

            for (name_buf[0..count]) |name| {
                // Apply namespace filtering
                const display_name = blk: {
                    if (ns_prefix.len == 0) {
                        // Default namespace — only bare names (no NUL separator)
                        if (std.mem.indexOfScalar(u8, name, ns_keys.NAMESPACE_SEPARATOR) != null) continue;
                        break :blk name;
                    } else {
                        if (!std.mem.startsWith(u8, name, ns_prefix)) continue;
                        break :blk name[ns_prefix.len..];
                    }
                };

                const gop = try seen.getOrPut(display_name);
                if (!gop.found_existing) {
                    const partitions = sp.getPartitionCount(display_name);
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("name", display_name);
                    try obj.stringField("namespace", effective_ns);
                    try obj.intField("partitions", partitions);
                    try obj.intField("ingest_rate", 0);
                    try obj.intField("reads", 0);
                    try obj.stringField("retention", "7d");
                    try obj.end();
                }
            }
        }
    }

    // Also include any streams from MetricsRegistry (e.g. from metrics-only sources)
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            const stream_entry = entry.value_ptr.*;
            if (ns_filter) |ns| {
                if (!std.mem.eql(u8, stream_entry.namespace, ns)) continue;
            }
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
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name - Stream detail with partitions and consumer groups
pub fn getStreamDetail(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

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

    // Partitions — logical record count (sum of batch sizes) from shard 0.
    {
        var parts_arr = try obj.arrayField("partitions");
        try parts_arr.begin();
        if (getShard(ctx, 0)) |shard| {
            const partition = shard.defaultPartition();
            const sp = &partition.stream;
            const name_hash = router.nameHash(router.namespaceHash(ns_q), stream_name);
            const record_count = streamLogicalCount(allocator, partition, sp, name_hash);
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

    // Consumer groups — only those belonging to THIS stream+namespace. Group keys
    // are `[ns\0]stream\0group`; filter by the stream's qualified prefix and emit
    // the bare group name plus its last-delivered position (for the activity bar).
    {
        var groups_arr = try obj.arrayField("consumer_groups");
        try groups_arr.begin();
        var sp_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const stream_prefix = ns_keys.qualifyKey(&sp_buf, ns_q, stream_name) catch stream_name;
        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getStreamProjection(ctx, i)) |sp| {
                var git = sp.groups.iterator();
                while (git.next()) |ge| {
                    const gkey = ge.key_ptr.*;
                    if (!std.mem.startsWith(u8, gkey, stream_prefix)) continue;
                    if (gkey.len <= stream_prefix.len or gkey[stream_prefix.len] != ns_keys.NAMESPACE_SEPARATOR) continue;
                    const bare_group = gkey[stream_prefix.len + 1 ..];
                    const grp = ge.value_ptr;
                    try groups_arr.next();
                    var gobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try gobj.begin();
                    try gobj.stringField("name", bare_group);
                    try gobj.intField("members", grp.members.count());
                    try gobj.intField("pending_count", grp.pelCount());
                    try gobj.intField("last_delivered_ms", @as(i64, @intCast(grp.last_delivered_id.timestamp_ms)));
                    try gobj.end();
                }
            }
        }
        try groups_arr.end();
    }

    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/messages - Paginated messages
/// Query params: ?cursor=<ts>-<seq>&limit=&partition=
/// cursor format: "<timestamp_ms>-<sequence>" — omit for start of stream
pub fn getStreamMessages(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 2000;
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

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

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var msg_count: u64 = 0;
    var total_count: u64 = 0;
    var last_id: StreamID = StreamID.MIN;

    {
        var msgs_arr = try obj.arrayField("messages");
        try msgs_arr.begin();

        // Read from shard 0's stream projection using StreamRecord, then resolve
        // each record's payload from the partition's UAL via the wrap-safe
        // readCopy (the zero-copy read silently skips ring-boundary-wrapping
        // entries — a data-loss hazard, see getEntry-wrap-unsafe).
        if (getShard(ctx, 0)) |shard| {
            const partition = shard.defaultPartition();
            const sp = &partition.stream;
            const name_hash = router.nameHash(router.namespaceHash(ns_q), stream_name);
            total_count = streamLogicalCount(allocator, partition, sp, name_hash);
            const cap: usize = @min(@as(usize, @intCast(limit)), 1000);

            const StreamRecord = stream_mod.StreamRecord;
            const buf = allocator.alloc(StreamRecord, cap) catch null;
            defer if (buf) |b| allocator.free(b);
            if (buf) |b| {
                const read_count = sp.readStreamAfter(name_hash, start_id, null, b);
                const cap_u: u64 = @intCast(cap);
                var payload_buf: [65536]u8 = undefined;
                var payloads: [256][]const u8 = undefined;
                outer: for (b[0..read_count]) |rec| {
                    if (msg_count >= cap_u) break;
                    // entry → CommandPayload.value → strip partition prefix → batch blob → N record payloads
                    var pn: usize = 0;
                    if (partition.ual.readCopy(rec.ual_index, &payload_buf)) |entry| {
                        if (entry.commandPayload()) |cmd| {
                            const batch = stream_mod.decodeAppendValue(cmd.value).payload;
                            pn = unpackBatchPayloads(batch, &payloads);
                        }
                    }
                    if (pn == 0) {
                        // Couldn't read/parse — still surface the record (id only).
                        try msgs_arr.next();
                        var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try mobj.begin();
                        try mobj.intField("id_ms", @as(i64, @intCast(rec.id.timestamp_ms)));
                        try mobj.intField("id_seq", @as(i64, @intCast(rec.id.sequence)));
                        try mobj.intField("ual_index", @as(i64, @intCast(rec.ual_index)));
                        try mobj.intField("size", 0);
                        try mobj.stringField("payload", "");
                        try mobj.end();
                        last_id = rec.id;
                        msg_count += 1;
                        continue;
                    }
                    for (payloads[0..pn], 0..) |raw, j| {
                        if (msg_count >= cap_u) break :outer;
                        const payload = if (std.unicode.utf8ValidateSlice(raw)) raw else "<binary>";
                        const seq = rec.id.sequence + j;
                        try msgs_arr.next();
                        var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try mobj.begin();
                        try mobj.intField("id_ms", @as(i64, @intCast(rec.id.timestamp_ms)));
                        try mobj.intField("id_seq", @as(i64, @intCast(seq)));
                        try mobj.intField("ual_index", @as(i64, @intCast(rec.ual_index)));
                        try mobj.intField("size", @as(i64, @intCast(payload.len)));
                        try mobj.stringField("payload", payload);
                        try mobj.end();
                        last_id = .{ .timestamp_ms = rec.id.timestamp_ms, .sequence = seq };
                        msg_count += 1;
                    }
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
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/groups/:group - Consumer group detail
pub fn getGroupDetail(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = query_string;
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    try obj.stringField("namespace", "default");

    // Find group across shard projections. Groups are keyed per-(stream, group)
    // as `qualifyGroupKey(ns, stream, group)`; the dashboard scopes to the
    // default namespace.
    var gk_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    const group_key = ns_keys.qualifyGroupKey(&gk_buf, "default", stream_name, group_name) catch group_name;
    var found = false;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            if (sp.getGroup(group_key)) |group| {
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
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/groups/:group/members - Consumer group members (flat array)
pub fn getGroupMembers(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = query_string;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var gk_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    const group_key = ns_keys.qualifyGroupKey(&gk_buf, "default", stream_name, group_name) catch group_name;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            if (sp.getGroup(group_key)) |group| {
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
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/groups/:group/pending - Pending messages
pub fn getGroupPending(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = query_string;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    const PendingEntry = @import("../../../projection/stream.zig").PendingEntry;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    var pending_arr = try obj.arrayField("pending");
    try pending_arr.begin();

    var gk_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    const group_key = ns_keys.qualifyGroupKey(&gk_buf, "default", stream_name, group_name) catch group_name;
    var pel_count: usize = 0;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getStreamProjection(ctx, i)) |sp| {
            if (sp.getGroup(group_key)) |group| {
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
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Tests
// =============================================================================

test "getStreams returns empty when no streams registered" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getStreams(allocator, null, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "getStreamDetail returns detail without shards" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getStreamDetail(allocator, "events", null, &ctx);
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
