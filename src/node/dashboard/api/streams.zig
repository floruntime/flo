//! Dashboard API — Stream & Consumer Group Endpoints
//!
//! - GET /streams                                     — All streams
//! - GET /streams/:name                               — Stream detail with partitions & groups
//! - GET /streams/:name/messages                      — Paginated messages from stream
//! - GET /streams/:name/groups/:group                 — Consumer group detail
//! - GET /streams/:name/groups/:group/members         — Consumer group members
//! - GET /streams/:name/groups/:group/pending         — Pending messages

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const routing = h.routing;
const Dispatcher = h.Dispatcher;
const Core = h.Core;
const MetricsRegistry = h.MetricsRegistry;
const Tier = h.Tier;
const StartOffset = h.StartOffset;
const BinaryReader = h.BinaryReader;

/// GET /streams - List all streams
pub fn getStreams(allocator: Allocator, dispatchers: []*Dispatcher, cores: ?[]*Core, metrics: *MetricsRegistry) ![]const u8 {
    _ = dispatchers;
    _ = metrics;

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    const discovered = try h.scanStreamsFromCores(allocator, cores, null);
    defer h.freeDiscoveredStreams(allocator, discovered);

    for (discovered) |stream| {
        try arr.next();

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", stream.name);
        try obj.stringField("namespace", stream.namespace);
        try obj.intField("partitions", stream.partition_count);
        try obj.intField("ingest_rate", 0);
        try obj.intField("reads", 0);
        try obj.stringField("retention", "7d");
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name - Stream detail with partitions and consumer groups
pub fn getStreamDetail(allocator: Allocator, stream_name: []const u8, dispatchers: []*Dispatcher, cores: ?[]*Core, metrics: *MetricsRegistry) ![]const u8 {
    _ = metrics;

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", stream_name);

    // Find stream namespace
    const discovered = try h.scanStreamsFromCores(allocator, cores, null);
    defer h.freeDiscoveredStreams(allocator, discovered);

    var found_ns: ?[]const u8 = null;
    for (discovered) |s| {
        if (std.mem.eql(u8, s.name, stream_name)) {
            found_ns = s.namespace;
            break;
        }
    }

    if (found_ns) |ns| {
        try obj.stringField("namespace", ns);
    }

    // Dispatch stream_info to get total count and byte size
    const ns_for_info = found_ns orelse "default";
    const info_hash = routing.hashKeyWithNamespace(ns_for_info, stream_name);
    const info_owner = dispatchers[0].router.owningCoreFromHash(info_hash);
    const info_dispatcher = if (info_owner < dispatchers.len) dispatchers[info_owner] else dispatchers[0];

    const info_result = info_dispatcher.dispatch(.{ .stream_info = .{
        .namespace = ns_for_info,
        .stream = stream_name,
    } }, 0, 0, null) catch null;

    var total_count: u64 = 0;
    var total_bytes: u64 = 0;
    var partition_count: u32 = 1;
    if (info_result) |ir| {
        switch (ir) {
            .stream_info => |si| {
                total_count = si.count;
                total_bytes = si.bytes;
                partition_count = si.partition_count;
            },
            else => {},
        }
    }
    try obj.intField("total_count", total_count);
    try obj.intField("total_bytes", total_bytes);

    // Partitions array
    var partitions_arr = try obj.arrayField("partitions");
    try partitions_arr.begin();
    for (0..partition_count) |p| {
        try partitions_arr.next();
        var part_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try part_obj.begin();
        try part_obj.intField("id", @as(u64, @intCast(p)));

        const part_hash = routing.hashStreamPartition(ns_for_info, stream_name, @intCast(p));
        const part_owner = dispatchers[0].router.owningCoreFromHash(part_hash);
        const part_dispatcher = if (part_owner < dispatchers.len) dispatchers[part_owner] else dispatchers[0];

        var part_count: u64 = 0;
        var part_bytes: u64 = 0;
        const part_info = part_dispatcher.dispatch(.{ .stream_info = .{
            .namespace = ns_for_info,
            .stream = stream_name,
        } }, 0, 0, null) catch null;
        if (part_info) |pi| {
            switch (pi) {
                .stream_info => |si| {
                    part_count = si.count;
                    part_bytes = si.bytes;
                },
                else => {},
            }
        }

        try part_obj.intField("message_count", part_count);
        try part_obj.intField("bytes", part_bytes);
        try part_obj.stringField("status", "healthy");
        try part_obj.end();
    }
    try partitions_arr.end();

    // Consumer groups - scan state engine for group keys
    var groups_arr = try obj.arrayField("consumer_groups");
    try groups_arr.begin();

    if (found_ns) |ns| {
        const c = cores orelse &[_]*Core{};
        var seen_groups: [64][]const u8 = undefined;
        var seen_count: usize = 0;

        for (c) |core| {
            var cg_prefix_buf: [256]u8 = undefined;
            const cg_prefix = std.fmt.bufPrint(&cg_prefix_buf, "ns:{s}:stream:{s}:0:group:", .{ ns, stream_name }) catch continue;
            var cg_iter = core.state_engine.scan(cg_prefix);

            while (cg_iter.next()) |entry| {
                const key = entry.key;
                if (!std.mem.endsWith(u8, key, ":offset")) continue;

                const after_prefix = key[cg_prefix.len..];
                const colon_idx = std.mem.indexOf(u8, after_prefix, ":") orelse continue;
                const group_name = after_prefix[0..colon_idx];
                if (group_name.len == 0) continue;

                var already_seen = false;
                for (seen_groups[0..seen_count]) |sg| {
                    if (std.mem.eql(u8, sg, group_name)) {
                        already_seen = true;
                        break;
                    }
                }
                if (already_seen) continue;
                if (seen_count < seen_groups.len) {
                    seen_groups[seen_count] = group_name;
                    seen_count += 1;
                }

                try groups_arr.next();
                var grp_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try grp_obj.begin();
                try grp_obj.stringField("name", group_name);

                var state_key_buf: [512]u8 = undefined;
                const state_key = std.fmt.bufPrint(&state_key_buf, "ns:{s}:stream:{s}:0:group:{s}:state", .{ ns, stream_name, group_name }) catch {
                    try grp_obj.intField("members", 0);
                    try grp_obj.intField("lag", 0);
                    try grp_obj.stringField("status", "active");
                    try grp_obj.end();
                    continue;
                };

                var member_count: u64 = 0;
                var generation: u64 = 0;
                for (c) |state_core| {
                    var gs_buf: [4096]u8 = undefined;
                    if (state_core.state_engine.get(state_key, &gs_buf) catch null) |state_val| {
                        if (state_val.len >= 12) {
                            generation = std.mem.readInt(u64, state_val[0..8], .little);
                            const consumer_count = if (state_val.len >= 16) std.mem.readInt(u32, state_val[12..16], .little) else 0;
                            member_count = consumer_count;
                        }
                        break;
                    }
                }

                try grp_obj.intField("members", member_count);
                try grp_obj.intField("generation", generation);
                try grp_obj.intField("lag", 0);
                try grp_obj.stringField("status", if (member_count > 0) "active" else "idle");
                try grp_obj.end();
            }
        }
    }

    try groups_arr.end();
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/messages - Read messages from a stream with pagination
/// Query params: ?offset=<sequence>&limit=<count>&partition=<n>
pub fn getStreamMessages(allocator: Allocator, stream_name: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "{\"messages\":[],\"offset\":0,\"limit\":500,\"count\":0}");

    const offset_seq = h.parseQueryParam(u32, query_string, "offset") orelse 0;
    const raw_limit = h.parseQueryParam(u32, query_string, "limit") orelse 500;
    const limit: u32 = @min(raw_limit, 5000);
    const partition: ?u32 = h.parseQueryParam(u32, query_string, "partition");

    const disc = try h.discoverStreamNamespace(allocator, cores, stream_name);
    defer h.freeDiscoveredStreams(allocator, disc.discovered);
    const ns = disc.ns;

    // Route to the correct shard
    const hash = routing.hashStreamPartition(ns, stream_name, partition orelse 0);
    const owner = dispatchers[0].router.owningCoreFromHash(hash);
    const target_dispatcher = if (owner < dispatchers.len) dispatchers[owner] else dispatchers[0];

    const start_offset: StartOffset = if (offset_seq > 0)
        .{ .stream_id = .{ .timestamp_ms = 0, .sequence = offset_seq } }
    else
        StartOffset.earliest();

    const result = target_dispatcher.dispatch(.{
        .stream_read = .{
            .namespace = ns,
            .stream = stream_name,
            .start = start_offset,
            .end = null,
            .count = limit,
            .block_ms = null,
            .partition = partition,
        },
    }, 0, 0, null) catch |err| {
        log.warn("Stream messages dispatch error: {}", .{err});
        return try allocator.dupe(u8, "{\"messages\":[],\"offset\":0,\"limit\":500,\"count\":0}");
    };

    // Get stream info for totals
    const info_result = target_dispatcher.dispatch(.{ .stream_info = .{
        .namespace = ns,
        .stream = stream_name,
    } }, 0, 0, null) catch null;

    var total_count: u64 = 0;
    var total_bytes: u64 = 0;
    if (info_result) |ir| {
        switch (ir) {
            .stream_info => |si| {
                total_count = si.count;
                total_bytes = si.bytes;
            },
            else => {},
        }
    }

    if (result) |res| {
        switch (res) {
            .stream_messages => |m| {
                defer allocator.free(m.data);
                return try wrapMessagesResponse(allocator, m.data, offset_seq, limit, total_count, total_bytes);
            },
            .err => |e| {
                log.warn("Stream messages error: {s}", .{e.message});
                return try emptyMessagesResponse(allocator, offset_seq, limit, total_count, total_bytes);
            },
            else => return try emptyMessagesResponse(allocator, offset_seq, limit, total_count, total_bytes),
        }
    }

    return try emptyMessagesResponse(allocator, offset_seq, limit, total_count, total_bytes);
}

/// Build empty paginated messages response
fn emptyMessagesResponse(allocator: Allocator, offset: u32, limit: u32, total_count: u64, total_bytes: u64) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    var arr = try obj.arrayField("messages");
    try arr.begin();
    try arr.end();
    try obj.intField("offset", offset);
    try obj.intField("limit", limit);
    try obj.intField("count", 0);
    try obj.intField("total_count", total_count);
    try obj.intField("total_bytes", total_bytes);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// Wrap parsed messages in a paginated response envelope
fn wrapMessagesResponse(allocator: Allocator, data: []const u8, offset: u32, limit: u32, total_count: u64, total_bytes: u64) ![]const u8 {
    if (data.len < 4) return try emptyMessagesResponse(allocator, offset, limit, total_count, total_bytes);

    const msg_count = std.mem.readInt(u32, data[0..4], .little);
    if (msg_count == 0) return try emptyMessagesResponse(allocator, offset, limit, total_count, total_bytes);

    const messages_json = try parseStreamMessagesToJson(allocator, data);
    defer allocator.free(messages_json);

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.field("messages");
    try writer.writeAll(messages_json);
    try obj.intField("offset", offset);
    try obj.intField("limit", limit);
    try obj.intField("count", msg_count);
    try obj.intField("total_count", total_count);
    try obj.intField("total_bytes", total_bytes);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// Parse wire-format stream_messages data into JSON array.
fn parseStreamMessagesToJson(allocator: Allocator, data: []const u8) ![]const u8 {
    if (data.len < 4) return try allocator.dupe(u8, "[]");

    const count = std.mem.readInt(u32, data[0..4], .little);
    if (count == 0) return try allocator.dupe(u8, "[]");

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var offset: usize = 4;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // sequence: u64
        if (offset + 8 > data.len) break;
        const sequence = std.mem.readInt(u64, data[offset..][0..8], .little);
        offset += 8;

        // timestamp_ms: i64
        if (offset + 8 > data.len) break;
        const timestamp_ms = std.mem.readInt(i64, data[offset..][0..8], .little);
        offset += 8;

        // tier: u8
        if (offset + 1 > data.len) break;
        const tier_byte = data[offset];
        offset += 1;
        const tier_str: []const u8 = switch (@as(Tier, @enumFromInt(tier_byte))) {
            .hot => "hot",
            .warm => "warm",
            .cold => "cold",
        };

        // partition: u32
        if (offset + 4 > data.len) break;
        const partition_val = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        // key_present: u8
        if (offset + 1 > data.len) break;
        const key_present = data[offset];
        offset += 1;

        // Skip partition key if present
        if (key_present != 0) {
            if (offset + 4 > data.len) break;
            const key_len = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            offset += key_len;
        }

        // payload_len: u32 + payload
        if (offset + 4 > data.len) break;
        const payload_len = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;
        if (offset + payload_len > data.len) break;
        const payload = data[offset..][0..payload_len];
        offset += payload_len;

        // header_count: u32 (skip headers)
        if (offset + 4 > data.len) break;
        const header_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;
        var hdr: u32 = 0;
        while (hdr < header_count) : (hdr += 1) {
            if (offset + 4 > data.len) break;
            const hk_len = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4 + hk_len;
            if (offset + 4 > data.len) break;
            const hv_len = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4 + hv_len;
        }

        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.intField("sequence", sequence);
        try obj.intField("timestamp_ms", timestamp_ms);
        try obj.stringField("tier", tier_str);
        try obj.intField("partition", partition_val);
        try obj.stringField("payload", payload);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Consumer Group Endpoints
// =============================================================================

/// GET /streams/:name/groups/:group - Consumer group detail
pub fn getGroupDetail(allocator: Allocator, stream_name: []const u8, group_name: []const u8, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    const disc = try h.discoverStreamNamespace(allocator, cores, stream_name);
    defer h.freeDiscoveredStreams(allocator, disc.discovered);
    const ns = disc.ns;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("stream", stream_name);
    try obj.stringField("group", group_name);
    try obj.stringField("namespace", ns);

    // Load GroupState from state engine
    const c = cores orelse &[_]*Core{};
    var state_key_buf: [512]u8 = undefined;
    const state_key = std.fmt.bufPrint(&state_key_buf, "ns:{s}:stream:{s}:0:group:{s}:state", .{ ns, stream_name, group_name }) catch {
        try obj.intField("generation", 0);
        try obj.intField("partition_count", 0);
        try obj.intField("member_count", 0);
        try obj.end();
        return try json_buf.toOwnedSlice(allocator);
    };

    var found_state = false;
    for (c) |core| {
        var gs_buf2: [4096]u8 = undefined;
        if (core.state_engine.get(state_key, &gs_buf2) catch null) |state_val| {
            found_state = true;
            if (state_val.len >= 16) {
                var reader = BinaryReader.init(state_val);
                const generation = reader.readU64() orelse 0;
                const group_partition_count = reader.readU32() orelse 0;
                const consumer_count = reader.readU32() orelse 0;

                try obj.intField("generation", generation);
                try obj.intField("partition_count", group_partition_count);
                try obj.intField("member_count", consumer_count);

                // Parse consumer entries
                var members_arr = try obj.arrayField("members");
                try members_arr.begin();

                var ci: u32 = 0;
                while (ci < consumer_count) : (ci += 1) {
                    const consumer_id = reader.readLenPrefixed(u32) orelse break;
                    const last_seen = reader.readI64() orelse break;
                    _ = reader.readI64(); // skip blocking_until
                    _ = reader.readU32(); // skip stale_sweep_count

                    try members_arr.next();
                    var m_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try m_obj.begin();
                    try m_obj.stringField("id", consumer_id);
                    try m_obj.intField("last_seen", last_seen);
                    try m_obj.end();
                }

                try members_arr.end();

                // Parse assignments array: i32[partition_count]
                var assignments_arr = try obj.arrayField("assignments");
                try assignments_arr.begin();
                var ai: u32 = 0;
                while (ai < group_partition_count) : (ai += 1) {
                    const assignment = reader.readI32() orelse break;
                    try assignments_arr.next();
                    try writer.print("{d}", .{assignment});
                }
                try assignments_arr.end();
            }
            break;
        }
    }

    if (!found_state) {
        try obj.intField("generation", 0);
        try obj.intField("partition_count", 0);
        try obj.intField("member_count", 0);
    }

    // Get pending count via dispatch
    if (dispatchers.len > 0) {
        const target = h.routeToShard(dispatchers, ns, stream_name);

        const pending_result = target.dispatch(.{ .group_pending = .{
            .namespace = ns,
            .stream = stream_name,
            .group = group_name,
        } }, 0, 0, null) catch null;

        if (pending_result) |pr| {
            switch (pr) {
                .group_pending => |pending| {
                    defer allocator.free(pending.data);
                    if (pending.data.len >= 4) {
                        const pending_count = std.mem.readInt(u32, pending.data[0..4], .little);
                        try obj.intField("pending_count", pending_count);
                    } else {
                        try obj.intField("pending_count", 0);
                    }
                },
                else => {
                    try obj.intField("pending_count", 0);
                },
            }
        } else {
            try obj.intField("pending_count", 0);
        }
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group/members - Consumer group members
pub fn getGroupMembers(allocator: Allocator, stream_name: []const u8, group_name: []const u8, cores: ?[]*Core) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    const disc = try h.discoverStreamNamespace(allocator, cores, stream_name);
    defer h.freeDiscoveredStreams(allocator, disc.discovered);
    const ns = disc.ns;

    const c = cores orelse return try allocator.dupe(u8, "[]");

    var state_key_buf: [512]u8 = undefined;
    const state_key = std.fmt.bufPrint(&state_key_buf, "ns:{s}:stream:{s}:0:group:{s}:state", .{ ns, stream_name, group_name }) catch {
        return try allocator.dupe(u8, "[]");
    };

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    for (c) |core| {
        var gs_buf3: [4096]u8 = undefined;
        if (core.state_engine.get(state_key, &gs_buf3) catch null) |state_val| {
            if (state_val.len >= 16) {
                var reader = BinaryReader.init(state_val);
                _ = reader.readU64(); // skip generation
                _ = reader.readU32(); // skip partition_count
                const consumer_count = reader.readU32() orelse 0;

                var ci: u32 = 0;
                while (ci < consumer_count) : (ci += 1) {
                    const consumer_id = reader.readLenPrefixed(u32) orelse break;
                    const last_seen = reader.readI64() orelse break;
                    const blocking_until = reader.readI64() orelse break;
                    const stale_count = reader.readU32() orelse break;

                    try arr.next();
                    var m_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try m_obj.begin();
                    try m_obj.stringField("id", consumer_id);
                    try m_obj.intField("last_seen", last_seen);
                    try m_obj.intField("blocking_until", blocking_until);
                    try m_obj.intField("stale_sweep_count", stale_count);
                    try m_obj.end();
                }
            }
            break;
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /streams/:name/groups/:group/pending - Pending messages for a consumer group
pub fn getGroupPending(allocator: Allocator, stream_name: []const u8, group_name: []const u8, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "{\"pending\":[],\"count\":0}");

    const disc = try h.discoverStreamNamespace(allocator, cores, stream_name);
    defer h.freeDiscoveredStreams(allocator, disc.discovered);
    const ns = disc.ns;

    const target = h.routeToShard(dispatchers, ns, stream_name);

    const result = target.dispatch(.{ .group_pending = .{
        .namespace = ns,
        .stream = stream_name,
        .group = group_name,
    } }, 0, 0, null) catch {
        return try allocator.dupe(u8, "{\"pending\":[],\"count\":0}");
    };

    if (result) |res| {
        switch (res) {
            .group_pending => |pending| {
                defer allocator.free(pending.data);
                return try parsePendingToJson(allocator, pending.data);
            },
            .err => return try allocator.dupe(u8, "{\"pending\":[],\"count\":0}"),
            else => return try allocator.dupe(u8, "{\"pending\":[],\"count\":0}"),
        }
    }

    return try allocator.dupe(u8, "{\"pending\":[],\"count\":0}");
}

/// Parse pending wire format into JSON.
fn parsePendingToJson(allocator: Allocator, data: []const u8) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    if (data.len < 4) {
        var arr = try obj.arrayField("pending");
        try arr.begin();
        try arr.end();
        try obj.intField("count", 0);
        try obj.end();
        return try json_buf.toOwnedSlice(allocator);
    }

    var reader = BinaryReader.init(data);
    const count = reader.readU32() orelse 0;

    var arr = try obj.arrayField("pending");
    try arr.begin();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // StreamID: 16 bytes (timestamp_ms:u64 + sequence:u64)
        const ts = reader.readU64() orelse break;
        const seq = reader.readU64() orelse break;

        const consumer = reader.readLenPrefixed(u32) orelse break;
        const delivered_at = reader.readI64() orelse break;
        const delivery_count = reader.readU32() orelse break;

        var id_buf: [64]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}-{d}", .{ ts, seq }) catch continue;

        try arr.next();
        var p_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try p_obj.begin();
        try p_obj.stringField("id", id_str);
        try p_obj.stringField("consumer", consumer);
        try p_obj.intField("delivered_at", delivered_at);
        try p_obj.intField("delivery_count", delivery_count);
        try p_obj.end();
    }

    try arr.end();
    try obj.intField("count", count);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}
