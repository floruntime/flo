//! A sink's write as a channel message: the entry its target's machine
//! applies, built on the pipeline's shard by namespace id, and the slot it
//! routes to. Built here, it carries what a client's write would: a write
//! its client path would refuse is refused here, before it is sent.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const router = @import("../node/router.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const kv_machine = @import("../kv/machine.zig");
const kv_handler = @import("../kv/handler.zig");
const qe = @import("../protocol/queue_entries.zig");
const queue_handler = @import("../queue/handler.zig");
const ts_mod = @import("../projection/ts.zig");
const stream_mod = @import("../projection/stream.zig");
const persistence = @import("../storage/persistence.zig");
const limits = @import("../protocol/limits.zig");

const EntryType = entry_mod.EntryType;
const NamespaceId = router.NamespaceId;

/// A write ready to send: the slot its target routes to, and its entry.
pub const Built = struct {
    slot: u16,
    entry_type: EntryType,
    payload: []const u8,
};

/// What a sink's write can't be, and why: said to the operator, and
/// counted, at the sender.
pub const Refused = struct { why: []const u8 };

pub const Result = union(enum) { built: Built, refused: Refused };

/// The largest payload a message carries, and so any builder writes.
pub const MAX_PAYLOAD = @import("../protocol/processing_entries.zig").MAX_SINK_PAYLOAD;

fn slotOf(ns: NamespaceId, key: []const u8) u16 {
    return @intCast(router.slot(router.routeNsKey(ns, key)));
}

/// A KV put of `value` under `key`.
pub fn kv(buf: []u8, ns: NamespaceId, key: []const u8, value: []const u8, ttl_ms: ?u64) Result {
    if (kv_handler.KVHandler.keyRefusal(key)) |why| return .{ .refused = .{ .why = why } };
    var kb: [limits.MAX_KV_KEY]u8 = undefined;
    const stored = kv_machine.Key.init(&kb, ns, key).stored;
    const payload = kv_entries.Put.encodeInto(buf, .{ .key = stored, .value = value, .ttl_ms = ttl_ms, .condition = .always, .cas_version = null }) catch
        return .{ .refused = .{ .why = "the value is too large for one write" } };
    return .{ .built = .{ .slot = slotOf(ns, key), .entry_type = .kv_put, .payload = payload } };
}

/// An enqueue of `value` to queue `name`, at this node's caps.
pub fn queue(buf: []u8, caps: queue_handler.Caps, ns: NamespaceId, name: []const u8, priority: u8, value: []const u8) Result {
    if (name.len == 0) return .{ .refused = .{ .why = "queue name is required" } };
    if (name.len > qe.QueueName.max_size) return .{ .refused = .{ .why = "queue name too long" } };
    if (value.len > qe.MAX_MESSAGE) return .{ .refused = .{ .why = "message too large: a queue message holds at most 64 KiB" } };
    const payload = qe.Enqueue.encodeInto(buf, queue_handler.enqueueValue(caps, ns, name, priority, 0, value)) catch
        return .{ .refused = .{ .why = "the message is too large for one write" } };
    return .{ .built = .{ .slot = slotOf(ns, name), .entry_type = .queue_enqueue, .payload = payload } };
}

/// An append of `value` to stream `name`, as one record of partition 0.
/// Its stored value is built in `scratch`, which the caller frees.
pub fn stream(buf: []u8, scratch: std.mem.Allocator, ns: NamespaceId, name: []const u8, value: []const u8) Result {
    if (name.len == 0) return .{ .refused = .{ .why = "stream name is required" } };
    if (name.len > persistence.MAX_ENTRY_KEY) return .{ .refused = .{ .why = "stream name too long" } };
    // [partition u32] then the batch: [count u32][len u32][payload][header_count u16].
    const stored = scratch.alloc(u8, 4 + 4 + 4 + value.len + 2) catch return .{ .refused = .{ .why = "out of memory" } };
    std.mem.writeInt(u32, stored[0..4], 0, .little);
    std.mem.writeInt(u32, stored[4..8], 1, .little);
    std.mem.writeInt(u32, stored[8..12], @intCast(value.len), .little);
    @memcpy(stored[12..][0..value.len], value);
    std.mem.writeInt(u16, stored[12 + value.len ..][0..2], 0, .little);
    const e = entry_mod.buildCommandEntry(.stream_append, entry_mod.Flags.SINK, 0, 0, 0, ns, name, stored, buf) orelse
        return .{ .refused = .{ .why = "the record is too large for one write" } };
    if (stream_mod.appendRefusal(e.payload)) |why| return .{ .refused = .{ .why = why } };
    return .{ .built = .{ .slot = slotOf(ns, name), .entry_type = .stream_append, .payload = e.payload } };
}

/// A point of `measurement`'s `field` at `timestamp_ns` (0: its entry's
/// stamp), with `tags` as a client's write sends them.
pub fn ts(buf: []u8, ns: NamespaceId, measurement: []const u8, field: []const u8, value: f64, timestamp_ns: u64, tags: []const u8) Result {
    if (measurement.len == 0) return .{ .refused = .{ .why = "measurement is required" } };
    if (measurement.len > persistence.MAX_ENTRY_KEY) return .{ .refused = .{ .why = "measurement name too long" } };
    var vb: [ts_mod.MAX_TAG_STRING + 512]u8 = undefined;
    const encoded = ts_mod.TsWriteValue.encode(&vb, .{ .value = value, .timestamp_ns = timestamp_ns, .field_name = field, .tags = tags }) orelse
        return .{ .refused = .{ .why = "ts write: field/tags too large" } };
    const e = entry_mod.buildCommandEntry(.ts_write, entry_mod.Flags.SINK, 0, 0, 0, ns, measurement, encoded, buf) orelse
        return .{ .refused = .{ .why = "the point is too large for one write" } };
    return .{ .built = .{ .slot = slotOf(ns, measurement), .entry_type = .ts_write, .payload = e.payload } };
}

const testing = std.testing;

test "sink payloads: each kind is the entry its target applies, routed by namespace id and name" {
    const ns: NamespaceId = .of(21);
    var buf: [4096]u8 = undefined;
    var diag: @import("../protocol/schema.zig").Diagnostic = .{};

    const k = kv(&buf, ns, "user:1", "v", 5).built;
    try testing.expectEqual(EntryType.kv_put, k.entry_type);
    try testing.expectEqual(slotOf(ns, "user:1"), k.slot);
    const put = try kv_entries.Put.decodeAll(k.payload, &diag);
    try testing.expectEqualSlices(u8, &ns.bytes(), put.key[0..4]);
    try testing.expectEqualStrings("user:1", put.key[4..]);
    try testing.expectEqual(@as(?u64, 5), put.ttl_ms);

    const caps: queue_handler.Caps = .{};
    const q = queue(&buf, caps, ns, "jobs", 3, "m").built;
    const enq = try qe.Enqueue.decodeAll(q.payload, &diag);
    try testing.expectEqual(ns.int(), enq.ns);
    try testing.expectEqualStrings("jobs", enq.queue);
    try testing.expectEqual(@as(u32, 3), enq.priority);
    try testing.expectEqual(caps.max_messages, enq.max_messages);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const s = stream(&buf, arena.allocator(), ns, "events", "payload").built;
    try testing.expect(stream_mod.appendRefusal(s.payload) == null);
    const cmd = entry_mod.CommandPayload.deserialize(s.payload).?;
    try testing.expectEqual(ns, cmd.namespace_id);
    try testing.expectEqualStrings("events", cmd.key);

    const t = ts(&buf, ns, "cpu", "value", 1.5, 9, "host=a").built;
    const tc = entry_mod.CommandPayload.deserialize(t.payload).?;
    try testing.expectEqualStrings("cpu", tc.key);
    const tv = ts_mod.TsWriteValue.decode(tc.value, 0).?;
    try testing.expectEqual(@as(f64, 1.5), tv.value);
    try testing.expectEqualStrings("host=a", tv.tags);
}

test "sink payloads: a write its client path would refuse is refused before it is sent" {
    var buf: [4096]u8 = undefined;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try testing.expect(kv(&buf, .default, "", "v", null) == .refused);
    try testing.expect(kv(&buf, .default, "a\x00b", "v", null) == .refused);
    try testing.expect(queue(&buf, .{}, .default, "", 0, "m") == .refused);
    try testing.expect(stream(&buf, arena.allocator(), .default, "", "x") == .refused);
    try testing.expect(ts(&buf, .default, "", "value", 1, 0, "") == .refused);
}
