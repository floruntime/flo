//! Streams' metadata on the contract: which streams exist, by what name,
//! with what partition count and retention. Their records are the stream
//! projection's (`projection/stream.zig`), applied by the stream handler;
//! this machine owns `stream_config` and follows `stream_append` and
//! `stream_delete`, so a stream's record here comes and goes with it.
//!
//! - `stream.meta`: each stream, keyed (ns id, name hash): its name, its
//!   settings, when it was made, and whether a create made it or only an
//!   append implied it. A stream exists while this record does.
//! - `stream.retention`: each stream with a retention policy, keyed alike,
//!   so the retention duty visits only those.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const router = @import("../node/router.zig");
const proto = @import("../protocol/proto.zig");
const MAX_QUALIFIED_KEY = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Record = schema.Record;
const Stored = delivery.Stored;

pub const NamespaceId = router.NamespaceId;

pub const Meta = Record(&.{
    .{ .name = "name", .T = schema.Bytes(MAX_QUALIFIED_KEY) },
    .{ .name = "partition_count", .T = Int(u32) },
    // 0 is no limit.
    .{ .name = "retention_age_s", .T = Int(u64) },
    .{ .name = "retention_count", .T = Int(u64) },
    // The stamp of the create or first append that made it.
    .{ .name = "created_ns", .T = Int(u64) },
    // Its settings were asked for, by a create or an alter, rather than
    // implied by an append: a create of it must ask for the same ones.
    .{ .name = "explicit", .T = schema.Bool },
});

pub const Metas = view_mod.Family(47, "stream.meta", .id, Stored(Meta));
pub const Retained = view_mod.Family(48, "stream.retention", .id, delivery.Empty);

/// A `stream_config` entry's value: a create or an alter. In an alter a 0
/// keeps what the stream has.
pub const StreamConfig = struct {
    op: enum(u8) { create, alter },
    partition_count: u32,
    retention_age_s: u64,
    retention_count: u64,

    pub const SIZE = 1 + 4 + 8 + 8;

    pub fn encode(c: StreamConfig, out: *[SIZE]u8) void {
        out[0] = @backingInt(c.op);
        std.mem.writeInt(u32, out[1..5], c.partition_count, .little);
        std.mem.writeInt(u64, out[5..13], c.retention_age_s, .little);
        std.mem.writeInt(u64, out[13..21], c.retention_count, .little);
    }

    pub fn decode(bytes: []const u8) ?StreamConfig {
        if (bytes.len != SIZE or bytes[0] > 1) return null;
        return .{
            .op = @fromBackingInt(@intCast(bytes[0])),
            .partition_count = std.mem.readInt(u32, bytes[1..5], .little),
            .retention_age_s = std.mem.readInt(u64, bytes[5..13], .little),
            .retention_count = std.mem.readInt(u64, bytes[13..21], .little),
        };
    }
};

pub const NO_STREAM = "stream not found";
pub const SETTINGS_DIFFER = "conflict: the stream exists with other settings; change them with stream alter";
pub const NAME_TAKEN = "conflict: another stream's name hashes as this one's; choose another name";

/// A stream's key within its namespace: its name's hash, as its records'
/// `StreamKey` holds it.
pub fn hashOf(name: []const u8) [8]u8 {
    const k = router.nsNameKey(.invalid, name);
    return k[4..12].*;
}

/// `k` in `ns`, as these families store it.
pub fn storeKey(buf: []u8, ns: NamespaceId, k: []const u8) []const u8 {
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

pub fn hasRetention(m: Meta.Value) bool {
    return m.retention_age_s > 0 or m.retention_count > 0;
}

// ─── Reads ───────────────────────────────────────────────────────────────

/// The stream `name` in `ns`, if it exists. A record whose name differs is
/// another stream's, whose name hashes alike: not this one.
pub fn metaOf(read: ReadView, ns: NamespaceId, name: []const u8) ?Meta.Value {
    const hash = hashOf(name);
    var kb: [12]u8 = undefined;
    const got = (read.get(Metas, storeKey(&kb, ns, &hash)) catch return null) orelse return null;
    return if (std.mem.eql(u8, got.v.name, name)) got.v else null;
}

/// A stream's partition count: 1 for one that doesn't exist.
pub fn partitionCount(read: ReadView, ns: NamespaceId, name: []const u8) u32 {
    return if (metaOf(read, ns, name)) |m| @max(1, m.partition_count) else 1;
}

// ─── Keys ────────────────────────────────────────────────────────────────

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn commandOf(e: *const Entry) ?entry_mod.CommandPayload {
    return entry_mod.CommandPayload.deserialize(e.payload);
}

/// The stream an entry names: a config's, an append's or a delete's key is
/// its name.
fn streamKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const cmd = commandOf(e) orelse return;
    if (cmd.namespace_id == .invalid) return;
    const hash = hashOf(cmd.key);
    try out.addIn(Metas.ID, cmd.namespace_id, &hash);
    try out.addIn(Retained.ID, cmd.namespace_id, &hash);
}

/// What a followed entry may touch: an append only its stream's record,
/// a delete its retention too.
fn followKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    if (typeOf(e) != .stream_append) return streamKeys(e, out);
    const cmd = commandOf(e) orelse return;
    if (cmd.namespace_id == .invalid) return;
    const hash = hashOf(cmd.key);
    try out.addIn(Metas.ID, cmd.namespace_id, &hash);
}

// ─── Apply ───────────────────────────────────────────────────────────────

/// A create of a stream with settings (from a create or an alter) is
/// answered ok if it asks for the same ones, as a retry does, and refused
/// if not: changing them is alter's. A create of one an append implied,
/// never altered, gives it the settings asked.
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    if (typeOf(e) != .stream_config) return error.Halt;
    const cmd = commandOf(e) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return error.Halt;
    const c = StreamConfig.decode(cmd.value) orelse return error.Halt;
    const hash = hashOf(cmd.key);
    const key = try inNs(view, cmd.namespace_id, &hash);
    const found = try view.get(Metas, key);
    if (found) |f| if (!std.mem.eql(u8, f.v.name, cmd.key)) return out.refuse(.conflict, .unclassified, NAME_TAKEN);
    const existing: ?Meta.Value = if (found) |f| f.v else null;
    const meta: Meta.Value = switch (c.op) {
        .create => blk: {
            if (existing) |m| if (m.explicit) {
                if (m.partition_count == @max(1, c.partition_count) and m.retention_age_s == c.retention_age_s and m.retention_count == c.retention_count) return;
                return out.refuse(.conflict, .unclassified, SETTINGS_DIFFER);
            };
            break :blk .{
                .name = cmd.key,
                .partition_count = @max(1, c.partition_count),
                .retention_age_s = c.retention_age_s,
                .retention_count = c.retention_count,
                .created_ns = if (existing) |m| m.created_ns else e.header.timestamp_ns,
                .explicit = true,
            };
        },
        .alter => blk: {
            var m = existing orelse return out.refuse(.not_found, .not_found, NO_STREAM);
            if (c.retention_age_s > 0) m.retention_age_s = c.retention_age_s;
            if (c.retention_count > 0) m.retention_count = c.retention_count;
            // It has settings now: a create must ask for them, as of one
            // a create made, or it would drop them.
            m.explicit = true;
            break :blk m;
        },
    };
    try view.put(Metas, key, .{ .v = meta });
    const rk = try inNs(view, cmd.namespace_id, &hash);
    if (hasRetention(meta)) try view.put(Retained, rk, .{}) else try view.delete(Retained, rk);
}

/// An append that applied made its stream if it had none; a delete that
/// applied removed it, and its retention with it.
fn observe(view: *View, e: *const Entry) ApplyError!void {
    const cmd = commandOf(e) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return error.Halt;
    const hash = hashOf(cmd.key);
    const key = try inNs(view, cmd.namespace_id, &hash);
    switch (typeOf(e)) {
        .stream_append => {
            // A name whose hash another's record holds shares its records
            // already (`StreamKey`): it is that stream.
            if (try view.get(Metas, key) != null) return;
            try view.put(Metas, key, .{ .v = .{ .name = cmd.key, .partition_count = 1, .retention_age_s = 0, .retention_count = 0, .created_ns = e.header.timestamp_ns, .explicit = false } });
        },
        .stream_delete => {
            try view.delete(Metas, key);
            try view.delete(Retained, try inNs(view, cmd.namespace_id, &hash));
        },
        else => return error.Halt,
    }
}

/// An append to a stream that exists changes nothing here.
fn needed(read: ReadView, e: *const Entry) bool {
    if (typeOf(e) != .stream_append) return true;
    const cmd = commandOf(e) orelse return true;
    const hash = hashOf(cmd.key);
    var kb: [12]u8 = undefined;
    return read.store.get(Metas.ID, storeKey(&kb, cmd.namespace_id, &hash)) == null;
}

fn inNs(view: *View, ns: NamespaceId, k: []const u8) ApplyError![]const u8 {
    const buf = try view.arena.alloc(u8, 4 + k.len);
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

pub const machine: machine_mod.Machine = .{
    .name = "streams",
    .entry_types = &.{.stream_config},
    .families = &.{ machine_mod.info(Metas), machine_mod.info(Retained) },
    .keys = streamKeys,
    .apply = apply,
    .observes = &.{ .stream_append, .stream_delete },
    .observe_keys = followKeys,
    .observe = observe,
    .observe_needed = needed,
    .namespace = commandNamespace,
};

fn commandNamespace(e: *const Entry) ?u32 {
    const cmd = commandOf(e) orelse return null;
    return cmd.namespace_id.int();
}

// ─── Tests ───────────────────────────────────────────────────────────────

const store_mod = @import("../contract/store.zig");
const testing = std.testing;

/// Streams' metadata on its own store, entries applied as the shard applies
/// them: a config through the machine, an append or a delete through its
/// observe.
const TestStreams = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init() TestStreams {
        return .{ .store = store_mod.Store.init(testing.allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
    }

    fn deinit(t: *TestStreams) void {
        t.store.deinit();
        t.scratch.deinit();
    }

    fn entry(t: *TestStreams, buf: []u8, etype: EntryType, name: []const u8, value: []const u8, stamp: u64) Entry {
        t.index += 1;
        return entry_mod.buildCommandEntry(etype, entry_mod.Flags.NONE, 1, t.index, stamp, .default, name, value, buf).?;
    }

    fn config(t: *TestStreams, c: StreamConfig, name: []const u8, stamp: u64) !Result {
        var value: [StreamConfig.SIZE]u8 = undefined;
        c.encode(&value);
        var pb: [256]u8 = undefined;
        const e = t.entry(&pb, .stream_config, name, &value, stamp);
        var out = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &out);
        return out;
    }

    fn follow(t: *TestStreams, etype: EntryType, name: []const u8, stamp: u64) !void {
        var pb: [256]u8 = undefined;
        const e = t.entry(&pb, etype, name, "", stamp);
        try machine_mod.observeEntry(&machine, &t.store, &t.scratch, null, &e, .entry);
    }

    fn meta(t: *TestStreams, name: []const u8) ?Meta.Value {
        return metaOf(.{ .store = &t.store }, .default, name);
    }

    /// Whether the retention index names `name`'s stream.
    fn retained(t: *TestStreams, name: []const u8) bool {
        var kb: [12]u8 = undefined;
        const hash = hashOf(name);
        return t.store.get(Retained.ID, storeKey(&kb, .default, &hash)) != null;
    }

    /// The streams the retention index names: each must have retention.
    fn retainedCount(t: *TestStreams) !usize {
        var n: usize = 0;
        const read: ReadView = .{ .store = &t.store };
        var it = read.range(Retained, "");
        while (try it.next()) |item| {
            const m = (try read.get(Metas, item.key)) orelse return error.TestUnexpectedResult;
            try testing.expect(hasRetention(m.v));
            n += 1;
        }
        return n;
    }
};

fn create(pc: u32, age_s: u64, count: u64) StreamConfig {
    return .{ .op = .create, .partition_count = pc, .retention_age_s = age_s, .retention_count = count };
}

fn alter(age_s: u64, count: u64) StreamConfig {
    return .{ .op = .alter, .partition_count = 0, .retention_age_s = age_s, .retention_count = count };
}

test "streams machine: a create makes a stream with its settings; the same create again is ok, other settings are refused, and an alter merges" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(create(3, 60, 0), "s", 10)).refused() == null);
    const m = t.meta("s").?;
    try testing.expectEqual(@as(u32, 3), m.partition_count);
    try testing.expectEqual(@as(u64, 60), m.retention_age_s);
    try testing.expectEqual(@as(u64, 10), m.created_ns);
    try testing.expect(m.explicit);
    try testing.expect((try t.config(create(3, 60, 0), "s", 20)).refused() == null);
    try testing.expectEqual(proto.StatusCode.conflict, (try t.config(create(1, 0, 0), "s", 30)).refused().?.status);
    try testing.expect((try t.config(alter(0, 7), "s", 40)).refused() == null);
    const merged = t.meta("s").?;
    try testing.expectEqual(@as(u32, 3), merged.partition_count);
    try testing.expectEqual(@as(u64, 60), merged.retention_age_s);
    try testing.expectEqual(@as(u64, 7), merged.retention_count);
    try testing.expectEqual(proto.StatusCode.not_found, (try t.config(alter(1, 0), "nope", 50)).refused().?.status);
    try testing.expect(t.meta("nope") == null);
}

test "streams machine: a first append makes its stream with no settings, a later one changes nothing, and a create then gives it settings" {
    var t = TestStreams.init();
    defer t.deinit();
    try t.follow(.stream_append, "s", 10);
    const m = t.meta("s").?;
    try testing.expect(!m.explicit);
    try testing.expectEqual(@as(u32, 1), m.partition_count);
    try testing.expectEqual(@as(u64, 10), m.created_ns);
    try t.follow(.stream_append, "s", 20);
    try testing.expectEqual(@as(u64, 10), t.meta("s").?.created_ns);
    try testing.expect((try t.config(create(2, 0, 9), "s", 30)).refused() == null);
    const made = t.meta("s").?;
    try testing.expect(made.explicit);
    try testing.expectEqual(@as(u32, 2), made.partition_count);
    try testing.expectEqual(@as(u64, 10), made.created_ns);
}

test "streams machine: the retention index names exactly the streams with a retention policy, through create, alter and delete" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(create(1, 0, 0), "plain", 1)).refused() == null);
    try testing.expect((try t.config(create(1, 30, 0), "aged", 2)).refused() == null);
    try t.follow(.stream_append, "implied", 3);
    try testing.expect(!t.retained("plain") and t.retained("aged") and !t.retained("implied"));
    try testing.expectEqual(@as(usize, 1), try t.retainedCount());
    // An alter gives one a policy; a refused create changes nothing.
    try testing.expect((try t.config(alter(0, 4), "plain", 4)).refused() == null);
    try testing.expect((try t.config(alter(0, 2), "implied", 5)).refused() == null);
    try testing.expect((try t.config(create(1, 99, 0), "aged", 6)).refused() != null);
    try testing.expect(t.retained("plain") and t.retained("implied"));
    try testing.expectEqual(@as(usize, 3), try t.retainedCount());
    // A delete takes the stream and its policy.
    try t.follow(.stream_delete, "aged", 7);
    try testing.expect(t.meta("aged") == null and !t.retained("aged"));
    try testing.expectEqual(@as(usize, 2), try t.retainedCount());
    // Made again, it starts with none.
    try t.follow(.stream_append, "aged", 8);
    try testing.expect(!t.retained("aged") and !t.meta("aged").?.explicit);
    try testing.expectEqual(@as(usize, 2), try t.retainedCount());
}

test "streams machine: a create of a name whose hash another stream's record holds is refused" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(create(1, 0, 0), "s", 1)).refused() == null);
    // Another name under the same key, as a hash collision would put it.
    const hash = hashOf("s");
    var kb: [12]u8 = undefined;
    const key = storeKey(&kb, .default, &hash);
    const other: Meta.Value = .{ .name = "other", .partition_count = 1, .retention_age_s = 0, .retention_count = 0, .created_ns = 1, .explicit = true };
    const bytes = try t.store.allocator.alloc(u8, try Meta.encodedSize(other));
    _ = try Meta.encodeInto(bytes, other);
    t.store.replace(Metas.ID, t.store.find(Metas.ID, key).?, bytes);
    try testing.expectEqual(proto.StatusCode.conflict, (try t.config(create(1, 0, 0), "s", 2)).refused().?.status);
    try testing.expect(t.meta("s") == null);
}

test "streams machine: an alter gives a stream an append made its settings, so a create that asks for others is refused and the policy stays" {
    var t = TestStreams.init();
    defer t.deinit();
    try t.follow(.stream_append, "s", 1);
    try testing.expect((try t.config(alter(3600, 0), "s", 2)).refused() == null);
    try testing.expect(t.meta("s").?.explicit);
    // "Append, then create to ensure", with no retention asked.
    try testing.expectEqual(proto.StatusCode.conflict, (try t.config(create(1, 0, 0), "s", 3)).refused().?.status);
    try testing.expectEqual(@as(u64, 3600), t.meta("s").?.retention_age_s);
    try testing.expect(t.retained("s"));
    // Asking for what it has is a retry: ok, nothing changed. A partition
    // count of 0 asks for 1, as the stored one is.
    try testing.expect((try t.config(create(0, 3600, 0), "s", 4)).refused() == null);
    try testing.expectEqual(@as(u64, 1), t.meta("s").?.created_ns);
}
