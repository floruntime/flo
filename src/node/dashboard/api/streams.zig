//! Dashboard API — Stream & Consumer Group Endpoints
//!
//! - GET /streams                                     — All streams
//! - GET /streams/:name                               — Stream detail with partitions
//! - GET /streams/:name/messages                      — Paginated messages from stream
//! - GET /streams/:name/groups/:group                 — Consumer group detail
//! - GET /streams/:name/groups/:group/members         — Consumer group members
//! - GET /streams/:name/groups/:group/pending         — Pending messages
//!
//! The dashboard asks each shard, as any client does: every read goes over
//! loopback to the node's own protocol port, and the shard that owns the
//! stream answers it.

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const loopback = @import("loopback.zig");
const output = @import("../../../cli/output.zig");
const client_mod = @import("../../../cli/client/mod.zig");
const StreamID = client_mod.StreamID;
const WireReader = client_mod.WireReader;

// ── Helpers ─────────────────────────────────────────────────────────

/// One read through the CLI's op client `f`, given the request's reader.
fn ask(reader: *loopback.Reader, comptime f: anytype, args: anytype) !client_mod.Response {
    return @call(.auto, f, .{try reader.next()} ++ args);
}

/// Cumulative append/read record counts for a (namespace, topic), summed across
/// all partition entries in the metrics registry. Returns zeros if the stream
/// has no registered metrics yet. Locks the registry once per call.
const StreamCounts = struct { ingest: u64 = 0, reads: u64 = 0 };
fn streamCounts(ctx: *DashboardContext, namespace: []const u8, topic: []const u8) StreamCounts {
    var c = StreamCounts{};
    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();
    var it = ctx.metrics.streams.iterator();
    while (it.next()) |entry| {
        const e = entry.value_ptr.*;
        if (!std.mem.eql(u8, e.topic, topic)) continue;
        if (!std.mem.eql(u8, e.namespace, namespace)) continue;
        const snap = e.metrics.snapshot();
        c.ingest += snap.append_records_total;
        c.reads += snap.read_records_total;
    }
    return c;
}

/// A `stream_info` answer: [first_ts:u64][first_seq:u64][last_ts:u64]
/// [last_seq:u64][count:u64][bytes:u64][partition_count:u32]
/// [retention_age_s:u64][retention_count:u64][retention_bytes:u64].
const StreamInfo = struct { count: u64, bytes: u64, partition_count: u32 };

fn parseInfo(data: []const u8) ?StreamInfo {
    var r = WireReader.init(data);
    for (0..4) |_| _ = r.readU64() orelse return null;
    const count = r.readU64() orelse return null;
    const bytes = r.readU64() orelse return null;
    const pc = r.readU32() orelse return null;
    return .{ .count = count, .bytes = bytes, .partition_count = pc };
}

/// One stream's `stream_info`, or the JSON answer saying why there is none.
fn streamInfo(allocator: Allocator, reader: *loopback.Reader, namespace: []const u8, stream: []const u8) !union(enum) { info: StreamInfo, answer: []const u8 } {
    var got = ask(reader, client_mod.stream.info, .{ namespace, stream }) catch |err|
        return .{ .answer = try loopback.readFailed(allocator, "stream info", err) };
    defer got.deinit();
    if (got.isError()) return .{ .answer = try h.jsonError(allocator, got.errorMessage()) };
    const info = parseInfo(got.data) orelse return .{ .answer = try h.jsonError(allocator, "stream info: malformed answer") };
    return .{ .info = info };
}

/// A `stream_group_info` answer: [pel_count:u64][member_count:u32][created_at_ns:u64].
const GroupInfo = struct { pending: u64, members: u32 };

fn parseGroupInfo(data: []const u8) ?GroupInfo {
    var r = WireReader.init(data);
    const pending = r.readU64() orelse return null;
    const members = r.readU32() orelse return null;
    return .{ .pending = pending, .members = members };
}

/// A name holding a NUL could spell another namespace's group: never asked.
fn hasNul(s: []const u8) bool {
    return std.mem.indexOfScalar(u8, s, 0) != null;
}

/// `stream_list` entries: [name_len:u32][name][partition_count:u32].
const stream_list_columns = [_]output.WireColumn{
    .{ .field = "name", .header = "", .field_type = .str_u32 },
    .{ .field = "partitions", .header = "", .field_type = .uint_u32 },
};

const StreamListAsker = struct {
    namespace: []const u8,
    pub fn ask(self: StreamListAsker, client: *client_mod.Client, cursor: ?[]const u8) !client_mod.Response {
        return client_mod.stream.list(client, self.namespace, null, cursor);
    }
};

/// GET /streams - A namespace's streams (?namespace=, default "default"),
/// from `stream_list`, which asks every shard. Retention is null: it would
/// take one `stream_info` per stream.
pub fn getStreams(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace");
    const effective_ns = ns_filter orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "stream list", err);
    defer reader.deinit();
    var pages = reader.listAll(allocator, &stream_list_columns, StreamListAsker{ .namespace = effective_ns }) catch |err|
        return try loopback.readFailed(allocator, "stream list", err);
    defer pages.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    var r = WireReader.init(pages.list());
    const count = r.readU32() orelse 0;
    for (0..count) |_| {
        const name = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, "stream list: malformed answer");
        const partitions = r.readU32() orelse return try h.jsonError(allocator, "stream list: malformed answer");
        const gop = try seen.getOrPut(name);
        if (gop.found_existing) continue;
        const counts = streamCounts(ctx, effective_ns, name);
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", name);
        try obj.stringField("namespace", effective_ns);
        try obj.intField("partitions", partitions);
        // Cumulative record counts (true per-second rate needs client-side
        // windowing across polls).
        try obj.intField("ingest_rate", counts.ingest);
        try obj.intField("reads", counts.reads);
        try obj.nullField("retention");
        try obj.end();
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
                // Counts come straight from this entry's snapshot (we
                // already hold the lock).
                const snap = stream_entry.metrics.snapshot();
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", stream_entry.topic);
                try obj.stringField("namespace", stream_entry.namespace);
                try obj.intField("partitions", 1);
                try obj.intField("ingest_rate", snap.append_records_total);
                try obj.intField("reads", snap.read_records_total);
                try obj.nullField("retention");
                try obj.end();
            }
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// POST /streams/:name/trim - trim a stream by count or age (loopback write).
/// Query: ?namespace= &max_len= &max_age_s= &dry_run=
pub fn trimStream(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    var why_buf: [96]u8 = undefined;
    if (h.queryRefusal(&why_buf, query_string, &.{ "namespace", "max_len", "max_age_s", "dry_run" })) |why| {
        return try h.jsonError(allocator, why);
    }
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const max_len = h.parseQueryParam(u64, query_string, "max_len");
    const max_age_s = h.parseQueryParam(u64, query_string, "max_age_s");
    const dry_run = h.boolQueryParam(query_string, "dry_run") catch
        return try h.jsonError(allocator, "dry_run must be true, false, 1 or 0");

    // A value that doesn't parse is refused, not read as absent.
    if (max_len == null and h.parseQueryParam([]const u8, query_string, "max_len") != null)
        return try h.jsonError(allocator, "max_len must be a whole number");
    if (max_age_s == null and h.parseQueryParam([]const u8, query_string, "max_age_s") != null)
        return try h.jsonError(allocator, "max_age_s must be a whole number");
    if (max_len != null and max_len.? == 0) return try h.jsonError(allocator, "max_len must be > 0");
    if (max_age_s != null and max_age_s.? == 0) return try h.jsonError(allocator, "max_age_s must be > 0");
    if (max_len != null and max_age_s != null) return try h.jsonError(allocator, "trim takes only one of max_len or max_age_s");
    if (max_len == null and max_age_s == null) {
        return try h.jsonError(allocator, "trim requires one of max_len or max_age_s");
    }

    var client = loopback.connect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.stream.trim(&client, ns_q, stream_name, max_len, null, max_age_s, dry_run) catch
        return try h.jsonError(allocator, "Trim failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());
    const t = client_mod.stream.parseTrimmed(resp) orelse return try h.jsonError(allocator, "Malformed trim response");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("stream", stream_name);
    try obj.boolField("dry_run", dry_run);
    try obj.intField("trimmed", t.removed);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// DELETE /streams/:name - delete a stream entirely (loopback write).
/// Query: ?namespace= &force=  (force required for non-empty streams)
pub fn deleteStream(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const force = h.boolQueryParam(query_string, "force") catch
        return try h.jsonError(allocator, "force must be true, false, 1 or 0");

    var client = loopback.connect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.stream.delete(&client, ns_q, stream_name, force) catch
        return try h.jsonError(allocator, "Delete failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("stream", stream_name);
    try obj.boolField("deleted", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// DELETE /streams/:name/groups/:group - delete a consumer group (loopback write).
pub fn deleteGroup(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var client = loopback.connect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.stream.groupDelete(&client, ns_q, stream_name, group_name) catch
        return try h.jsonError(allocator, "Group delete failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    try obj.boolField("deleted", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name - Stream detail with partitions, from `stream_info`
/// (answered by the shard that owns the stream). No op lists a stream's
/// consumer groups yet: `consumer_groups` is null.
pub fn getStreamDetail(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "stream info", err);
    defer reader.deinit();
    const info = switch (try streamInfo(allocator, &reader, ns_q, stream_name)) {
        .info => |i| i,
        .answer => |a| return a,
    };

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", stream_name);
    try obj.stringField("namespace", ns_q);
    try obj.intField("total_count", info.count);
    try obj.intField("total_bytes", info.bytes);

    // Counts are the stream's, not each partition's: a partition's own
    // shows only when the stream has one.
    {
        var parts_arr = try obj.arrayField("partitions");
        try parts_arr.begin();
        const one = info.partition_count == 1;
        for (0..info.partition_count) |p| {
            try parts_arr.next();
            var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try pobj.begin();
            try pobj.intField("id", p);
            if (one) try pobj.intField("record_count", info.count) else try pobj.nullField("record_count");
            if (one) try pobj.intField("stored_bytes", info.bytes) else try pobj.nullField("stored_bytes");
            try pobj.end();
        }
        try parts_arr.end();
    }
    try obj.nullField("consumer_groups");

    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/messages - Paginated messages, from `stream_read`.
/// Query params: ?cursor=<ts>-<seq>&limit=&namespace=
/// cursor format: "<timestamp_ms>-<sequence>", the last id seen; omit for
/// the start of the stream.
pub fn getStreamMessages(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 2000;
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    const cursor_str = h.parseQueryParam([]const u8, query_string, "cursor");
    const start_id = if (cursor_str) |cs| blk: {
        const sep_pos = std.mem.indexOfScalar(u8, cs, '-') orelse break :blk StreamID.MIN;
        const ts = std.fmt.parseInt(u64, cs[0..sep_pos], 10) catch break :blk StreamID.MIN;
        const seq = std.fmt.parseInt(u64, cs[sep_pos + 1 ..], 10) catch break :blk StreamID.MIN;
        break :blk StreamID{ .timestamp_ms = ts, .sequence = seq };
    } else StreamID.MIN;

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "stream read", err);
    defer reader.deinit();
    const info = switch (try streamInfo(allocator, &reader, ns_q, stream_name)) {
        .info => |i| i,
        .answer => |a| return a,
    };
    // The read takes the records after `start_id`, whole appends within
    // `limit`, and waits for none.
    var got = ask(&reader, client_mod.stream.read, .{ ns_q, stream_name, client_mod.stream.StartMode.stream_id, @as(?StreamID, start_id), @as(?StreamID, null), @as(?u32, limit), @as(?u32, null), @as(?u32, null), @as(?[]const u8, null) }) catch |err|
        return try loopback.readFailed(allocator, "stream read", err);
    defer got.deinit();
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var msg_count: u64 = 0;
    var last_id: StreamID = StreamID.MIN;
    {
        var msgs_arr = try obj.arrayField("messages");
        try msgs_arr.begin();
        // [count:u32]([sequence:u64][timestamp_ms:i64][tier:u8][partition:u32]
        //   [key_present:u8]([key_len:u32][key])?[payload_len:u32][payload]
        //   [header_count:u32]([key_len:u32][key][value_len:u32][value])*)*
        const malformed = "stream read: malformed answer";
        var r = WireReader.init(got.data);
        const count = r.readU32() orelse 0;
        for (0..count) |_| {
            const seq = r.readU64() orelse return try h.jsonError(allocator, malformed);
            const ts = r.readI64() orelse return try h.jsonError(allocator, malformed);
            _ = r.readU8() orelse return try h.jsonError(allocator, malformed); // tier
            _ = r.readU32() orelse return try h.jsonError(allocator, malformed); // partition
            const key_present = r.readU8() orelse return try h.jsonError(allocator, malformed);
            if (key_present != 0) _ = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, malformed);
            const raw = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, malformed);
            const header_count = r.readU32() orelse return try h.jsonError(allocator, malformed);
            for (0..header_count) |_| {
                _ = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, malformed);
                _ = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, malformed);
            }
            const payload = if (std.unicode.utf8ValidateSlice(raw)) raw else "<binary>";
            try msgs_arr.next();
            var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try mobj.begin();
            try mobj.intField("id_ms", ts);
            try mobj.intField("id_seq", seq);
            try mobj.nullField("ual_index");
            try mobj.intField("size", payload.len);
            try mobj.stringField("payload", payload);
            try mobj.end();
            last_id = .{ .timestamp_ms = @intCast(@max(ts, 0)), .sequence = seq };
            msg_count += 1;
        }
        try msgs_arr.end();
    }

    // Next cursor — only present if we returned records
    if (msg_count > 0) {
        var cursor_buf: [64]u8 = undefined;
        const cursor_slice = std.fmt.bufPrint(&cursor_buf, "{d}-{d}", .{ last_id.timestamp_ms, last_id.sequence }) catch "";
        try obj.stringField("next_cursor", cursor_slice);
    }

    try obj.intField("limit", limit);
    try obj.intField("count", msg_count);
    try obj.intField("total_count", info.count);
    try obj.intField("total_bytes", info.bytes);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/groups/:group - Consumer group detail, from
/// `stream_group_info`. No op lists a group's members yet: `members` is null.
pub fn getGroupDetail(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    if (hasNul(stream_name) or hasNul(group_name)) return error.NotFound;

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "group info", err);
    defer reader.deinit();
    var got = ask(&reader, client_mod.stream.groupInfo, .{ ns_q, stream_name, group_name }) catch |err|
        return try loopback.readFailed(allocator, "group info", err);
    defer got.deinit();
    // An unknown group answers as an empty one.
    const info: GroupInfo = if (got.isNotFound())
        .{ .pending = 0, .members = 0 }
    else if (got.isError())
        return try h.jsonError(allocator, got.errorMessage())
    else
        parseGroupInfo(got.data) orelse return try h.jsonError(allocator, "group info: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    try obj.stringField("namespace", ns_q);
    try obj.intField("generation", 0);
    try obj.intField("partition_count", 1);
    try obj.intField("member_count", info.members);
    try obj.nullField("members");
    {
        var assignments_arr = try obj.arrayField("assignments");
        try assignments_arr.begin();
        try assignments_arr.end();
    }
    try obj.intField("pending_count", info.pending);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /streams/:name/groups/:group/members - No op lists a group's members
/// yet, so this says so rather than answer an empty list.
pub fn getGroupMembers(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = stream_name;
    _ = group_name;
    _ = query_string;
    _ = ctx;
    return try h.jsonError(allocator, "group members are not available yet");
}

/// GET /streams/:name/groups/:group/pending - Pending messages, from
/// `stream_group_pending` (at most one read's worth) and the group's whole
/// count from `stream_group_info`. `delivered_at_ms` is null: the answer
/// doesn't carry it.
pub fn getGroupPending(allocator: Allocator, stream_name: []const u8, group_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    if (hasNul(stream_name) or hasNul(group_name)) return error.NotFound;

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "group pending", err);
    defer reader.deinit();
    var info_got = ask(&reader, client_mod.stream.groupInfo, .{ ns_q, stream_name, group_name }) catch |err|
        return try loopback.readFailed(allocator, "group info", err);
    defer info_got.deinit();
    var got = ask(&reader, client_mod.stream.groupPending, .{ ns_q, stream_name, group_name }) catch |err|
        return try loopback.readFailed(allocator, "group pending", err);
    defer got.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    var pending_arr = try obj.arrayField("pending");
    try pending_arr.begin();

    // An unknown group answers as an empty one.
    var pel_count: u64 = 0;
    if (!info_got.isNotFound() and !got.isNotFound()) {
        if (info_got.isError()) return try h.jsonError(allocator, info_got.errorMessage());
        if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
        const malformed = "group pending: malformed answer";
        pel_count = (parseGroupInfo(info_got.data) orelse return try h.jsonError(allocator, malformed)).pending;
        // [count:u32]([timestamp_ms:u64][sequence:u64][delivery_count:u32][consumer_len:u16][consumer])*
        var r = WireReader.init(got.data);
        const count = r.readU32() orelse return try h.jsonError(allocator, malformed);
        for (0..count) |_| {
            const ts = r.readU64() orelse return try h.jsonError(allocator, malformed);
            const seq = r.readU64() orelse return try h.jsonError(allocator, malformed);
            const deliveries = r.readU32() orelse return try h.jsonError(allocator, malformed);
            const consumer = r.readLengthPrefixed(u16) orelse return try h.jsonError(allocator, malformed);
            try pending_arr.next();
            var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try pobj.begin();
            try pobj.intField("id_ms", ts);
            try pobj.intField("id_seq", seq);
            try pobj.stringField("consumer", consumer);
            try pobj.nullField("delivered_at_ms");
            try pobj.intField("delivery_count", deliveries);
            try pobj.end();
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

/// A context whose node port refuses every connect.
fn unreachableNode(metrics: *h.MetricsRegistry) DashboardContext {
    var ctx = DashboardContext.init(std.testing.allocator, metrics, 1);
    ctx.listen_port = 1; // nothing listens here
    return ctx;
}

test "stream reads say the node didn't answer, never an empty list" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableNode(&metrics);

    const answers = [_][]const u8{
        try getStreams(allocator, null, &ctx),
        try getStreamDetail(allocator, "events", null, &ctx),
        try getStreamMessages(allocator, "events", null, &ctx),
        try getGroupDetail(allocator, "events", "g", null, &ctx),
        try getGroupPending(allocator, "events", "g", null, &ctx),
    };
    defer for (answers) |a| allocator.free(a);
    for (answers) |a| {
        try std.testing.expect(std.mem.startsWith(u8, a, "{\"error\":"));
        try std.testing.expect(std.mem.indexOf(u8, a, "failed") != null);
    }
}

test "stream group members say they are not available yet" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableNode(&metrics);

    const result = try getGroupMembers(allocator, "events", "g", null, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"error\":\"group members are not available yet\"}", result);
}

test "stream group reads refuse a name holding a NUL before they connect" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableNode(&metrics);

    try std.testing.expectError(error.NotFound, getGroupDetail(allocator, "events", "a\x00b", null, &ctx));
    try std.testing.expectError(error.NotFound, getGroupPending(allocator, "e\x00v", "g", null, &ctx));
}

test "stream info answers parse; a short one doesn't" {
    var buf: [76]u8 = @splat(0);
    std.mem.writeInt(u64, buf[32..40], 7, .little);
    std.mem.writeInt(u64, buf[40..48], 99, .little);
    std.mem.writeInt(u32, buf[48..52], 3, .little);
    const info = parseInfo(&buf).?;
    try std.testing.expectEqual(@as(u64, 7), info.count);
    try std.testing.expectEqual(@as(u64, 99), info.bytes);
    try std.testing.expectEqual(@as(u32, 3), info.partition_count);
    try std.testing.expect(parseInfo(buf[0..40]) == null);
}
