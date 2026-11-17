//! The sinks machine: each pipeline's source cursor, and the sink writes
//! that move it.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const pe = @import("../protocol/processing_entries.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Record = schema.Record;

/// A pipeline's source cursor, keyed by its job's namespace and its
/// pipeline key (`pe.SinkBatch`).
pub const Cursor = Record(&.{
    .{ .name = "a", .T = Int(u64) },
    .{ .name = "b", .T = Int(u64) },
});

/// The last family id below `store.MAX_FAMILIES`: the next machine's
/// family needs that raised.
pub const CursorFam = view_mod.Family(63, "processing.cursor", .id, delivery.Stored(Cursor));

pub const At = struct { a: u64 = 0, b: u64 = 0 };

/// Pipeline `key`'s cursor in namespace `ns` as applied, or (0, 0) for none.
pub fn cursorOf(read: ReadView, ns: NamespaceId, key: []const u8) At {
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, key) catch return .{};
    const v = (read.get(CursorFam, k) catch null) orelse return .{};
    return .{ .a = v.v.a, .b = v.v.b };
}

fn typeOf(e: *const Entry) ?EntryType {
    return std.enums.fromInt(EntryType, e.header.entry_type);
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    const p = pe.SinkBatch.decodeAll(e.payload, &diag) catch return;
    try out.addIn(CursorFam.ID, .of(p.ns), p.pipeline);
}

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    _ = out;
    var diag: schema.Diagnostic = .{};
    // Only a leader makes these, from its own pipelines: one that doesn't
    // decode is a Flo bug.
    const p = pe.SinkBatch.decodeAll(e.payload, &diag) catch return error.Halt;
    const ns: NamespaceId = .of(p.ns);
    if (ns == .invalid) return error.Halt;
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, p.pipeline) catch return error.Halt;
    const at: At = if (try view.get(CursorFam, k)) |v| .{ .a = v.v.a, .b = v.v.b } else .{};
    // Its cursor moved on: applied already, or left behind by a leader change.
    if (at.a != p.from_a or at.b != p.from_b) return;
    try view.put(CursorFam, k, .{ .v = .{ .a = p.to_a, .b = p.to_b } });
    var it = p.messages.iterator();
    while (it.next() catch return error.Halt) |m| {
        if (m.flags != entry_mod.Flags.SINK) return error.Halt;
        try view.sendFlagged(.user(.of(m.ns)), m.slot, m.entry_type, m.flags, m.payload);
    }
}

/// A job removed: the cursors of its pipelines go with it. Its key, or its
/// key then `\0` and a source's index.
fn observeKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
    if (cmd.namespace_id == .invalid) return;
    try out.addIn(CursorFam.ID, cmd.namespace_id, cmd.key);
    var pb: [pe.MAX_PIPELINE_KEY + 1]u8 = undefined;
    if (cmd.key.len + 1 > pb.len) return;
    @memcpy(pb[0..cmd.key.len], cmd.key);
    pb[cmd.key.len] = 0;
    out.addRangeIn(CursorFam.ID, cmd.namespace_id, pb[0 .. cmd.key.len + 1]) catch |err| switch (err) {
        error.NoNamespace => unreachable,
        else => |o| return o,
    };
}

fn observe(view: *View, e: *const Entry) ApplyError!void {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len + 1 > pe.MAX_PIPELINE_KEY) return;
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    try view.delete(CursorFam, view_mod.idKey(&kb, cmd.namespace_id, cmd.key) catch return error.Halt);
    var pb: [4 + pe.MAX_PIPELINE_KEY + 1]u8 = undefined;
    const prefix = view_mod.idKey(&pb, cmd.namespace_id, cmd.key) catch return error.Halt;
    pb[prefix.len] = 0;
    try view.deleteRange(CursorFam, pb[0 .. prefix.len + 1], null);
}

pub const machine: machine_mod.Machine = .{
    .name = "sinks",
    .entry_types = &.{.processing_sink_batch},
    .families = &.{machine_mod.info(CursorFam)},
    .keys = keys,
    .apply = apply,
    .observes = &.{.processing_remove},
    .observe_keys = observeKeys,
    .observe = observe,
};

const testing = std.testing;
const store_mod = @import("../contract/store.zig");
const channels = @import("../contract/channels.zig");
const schema_w = schema.Writer;

const T = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init() T {
        var t: T = .{ .store = store_mod.Store.init(testing.allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
        t.store.place = .{ .group = 0, .count = 2 };
        t.store.strict_keys = true;
        return t;
    }

    fn deinit(t: *T) void {
        t.store.deinit();
        t.scratch.deinit();
    }

    fn entry(t: *T, etype: EntryType, payload: []const u8) Entry {
        t.index += 1;
        var h = std.mem.zeroes(entry_mod.Header);
        h.entry_type = @backingInt(etype);
        h.index = t.index;
        h.payload_len = @intCast(payload.len);
        return .{ .header = h, .payload = payload };
    }

    /// A batch for pipeline "job" moving its cursor `from` to `to`, sending
    /// one write per body to slot 1 (group 1's).
    fn batch(t: *T, from: At, to: At, bodies: []const []const u8) !void {
        var mb: [1024]u8 = undefined;
        var w: schema_w = .{ .buf = &mb };
        for (bodies) |b| try pe.SinkMessage.encode(&w, .{ .slot = 1, .entry_type = @backingInt(EntryType.kv_put), .flags = entry_mod.Flags.SINK, .ns = 1, .payload = b });
        var buf: [2048]u8 = undefined;
        const payload = try pe.SinkBatch.encodeInto(&buf, .{ .ns = 21, .pipeline = "job", .from_a = from.a, .from_b = from.b, .to_a = to.a, .to_b = to.b, .messages = .{ .bytes = w.written(), .count = @intCast(bodies.len) } });
        const e = t.entry(.processing_sink_batch, payload);
        var r = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r);
        try testing.expect(r.refused() == null);
    }

    fn read(t: *T) ReadView {
        return .{ .store = &t.store };
    }

    fn sent(t: *T) u64 {
        return channels.metaOf(t.read(), 1).next_seq - 1;
    }
};

test "sinks: a batch moves its pipeline's cursor and sends its writes once; one whose cursor moved on changes nothing" {
    var t = T.init();
    defer t.deinit();
    try t.batch(.{}, .{ .a = 5, .b = 1 }, &.{ "w1", "w2" });
    try testing.expectEqual(At{ .a = 5, .b = 1 }, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(@as(u64, 2), t.sent());
    // The same batch again (a leader change proposed it twice): nothing.
    try t.batch(.{}, .{ .a = 5, .b = 1 }, &.{ "w1", "w2" });
    try testing.expectEqual(@as(u64, 2), t.sent());
    // From where the first left it: applies.
    try t.batch(.{ .a = 5, .b = 1 }, .{ .a = 9, .b = 0 }, &.{"w3"});
    try testing.expectEqual(At{ .a = 9, .b = 0 }, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(@as(u64, 3), t.sent());
    // An older one, applied late: nothing.
    try t.batch(.{ .a = 5, .b = 1 }, .{ .a = 7, .b = 0 }, &.{"stale"});
    try testing.expectEqual(At{ .a = 9, .b = 0 }, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(@as(u64, 3), t.sent());
}

test "sinks: removing a job drops its pipelines' cursors, and no other job's" {
    var t = T.init();
    defer t.deinit();
    try t.batch(.{}, .{ .a = 1, .b = 1 }, &.{});
    // Two more pipelines: the job's second source, and another job.
    const keys_ = [_][]const u8{ "job\x001", "jobber" };
    for (keys_) |k| {
        var buf: [256]u8 = undefined;
        const payload = try pe.SinkBatch.encodeInto(&buf, .{ .ns = 21, .pipeline = k, .from_a = 0, .from_b = 0, .to_a = 2, .to_b = 2, .messages = .{ .bytes = "", .count = 0 } });
        const e = t.entry(.processing_sink_batch, payload);
        var r = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r);
    }
    var cb: [64]u8 = undefined;
    const remove = entry_mod.buildCommandEntry(.processing_remove, 0, 1, t.index + 1, 0, .of(21), "job", "", &cb).?;
    try machine_mod.observeEntry(&machine, &t.store, &t.scratch, null, &remove);
    try testing.expectEqual(At{}, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(At{}, cursorOf(t.read(), .of(21), "job\x001"));
    try testing.expectEqual(At{ .a = 2, .b = 2 }, cursorOf(t.read(), .of(21), "jobber"));
}

test "sinks: a batch whose write carries a flag other than the sink's halts: only a leader's own pipelines make these" {
    var t = T.init();
    defer t.deinit();
    var mb: [256]u8 = undefined;
    var w: schema_w = .{ .buf = &mb };
    try pe.SinkMessage.encode(&w, .{ .slot = 1, .entry_type = @backingInt(EntryType.kv_put), .flags = 0, .ns = 1, .payload = "x" });
    var buf: [512]u8 = undefined;
    const payload = try pe.SinkBatch.encodeInto(&buf, .{ .ns = 21, .pipeline = "job", .from_a = 0, .from_b = 0, .to_a = 1, .to_b = 0, .messages = .{ .bytes = w.written(), .count = 1 } });
    const e = t.entry(.processing_sink_batch, payload);
    var r = Result.start(&e);
    try testing.expectError(error.Halt, machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r));
}
