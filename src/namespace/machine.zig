//! The namespaces machine: every namespace shard 0's log created, and the
//! next id a create may take. Logged on shard 0 alone, the namespaces' one
//! owner; other shards read names from the node's table, which shard 0
//! publishes once a create is on disk.
//!
//! - `namespace.name`, keyed by name: the id it was given, its created-at
//!   stamp and the log index that created it.
//! - `namespace.meta`, one record: the next id and how many namespaces are
//!   live, "default" among them. They are kept apart because ids are never
//!   reused: the room is counted by `live`, and an id is only ever taken
//!   from `next_id`, which only rises.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const namespace_entries = @import("../protocol/namespace_entries.zig");
const ns_table = @import("table.zig");

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

/// The longest namespace name.
pub const MAX_NAME_LEN: usize = @import("../protocol/limits.zig").MAX_NAMESPACE_NAME;

/// Live namespaces, "default" among them.
pub const MAX_LIVE: u32 = ns_table.MAX_LIVE;

/// Why `name` cannot name a namespace, or null if it can: 1–63 of
/// `[a-z0-9_-]`, starting with a letter or digit and not ending in `-`; a
/// leading `_` is the system's. Lowercase only, so no two names differ by
/// case alone. The applier checks it too, so it is part of the replicated
/// state: loosening it later is safe, tightening it is not.
pub fn nameRefusal(name: []const u8) ?[]const u8 {
    if (name.len == 0) return "namespace name is required";
    if (name.len > MAX_NAME_LEN) return std.fmt.comptimePrint("namespace name too long (at most {d} bytes)", .{MAX_NAME_LEN});
    if (name[0] == '_') return "reserved namespace name: a leading '_' is the system's";
    for (name) |c| switch (c) {
        'a'...'z', '0'...'9', '-', '_' => {},
        'A'...'Z' => return "invalid namespace name: lowercase only",
        else => return "invalid namespace name: letters, digits, '_' and '-' only",
    };
    if (name[0] == '-') return "invalid namespace name: must start with a letter or digit";
    if (name[name.len - 1] == '-') return "invalid namespace name: must not end with '-'";
    return null;
}

pub fn isDefault(name: []const u8) bool {
    return std.mem.eql(u8, name, "default");
}

// ─── Stored records ────────────────────────────────────────────────────────

pub const Meta = Record(&.{
    .{ .name = "id", .T = Int(u32) },
    .{ .name = "created_at_ns", .T = Int(u64) },
    .{ .name = "created_index", .T = Int(u64) },
});

pub const Counts = Record(&.{
    .{ .name = "next_id", .T = Int(u32) },
    .{ .name = "live", .T = Int(u32) },
});

pub const NameFam = view_mod.Family(4, "namespace.name", .shard, Stored(Meta));
pub const CountsFam = view_mod.Family(5, "namespace.meta", .shard, Stored(Counts));

/// What `namespace.meta` holds before any create: "default", which has no
/// entry, is live, and no id is taken.
pub const INITIAL: Counts.Value = .{ .next_id = ns_table.FIRST_ALLOCATED.int(), .live = 1 };

const COUNTS_KEY = "ids";

/// A name's key in `namespace.name`: no namespace, then the name, so the
/// family iterates in name order.
pub fn nameKey(buf: *[1 + MAX_NAME_LEN]u8, name: []const u8) []const u8 {
    return view_mod.key(buf, "", name) catch unreachable; // a valid name fits
}

/// The prefix every `namespace.name` key starts with.
pub const NAME_PREFIX = "\x00";

fn countsKey(buf: *[1 + COUNTS_KEY.len]u8) []const u8 {
    return view_mod.key(buf, "", COUNTS_KEY) catch unreachable;
}

// ─── Reads ─────────────────────────────────────────────────────────────────

pub fn counts(read: ReadView) Counts.Value {
    var kb: [1 + COUNTS_KEY.len]u8 = undefined;
    const got = read.get(CountsFam, countsKey(&kb)) catch unreachable; // written by this machine alone
    return if (got) |s| s.v else INITIAL;
}

/// `name`'s record, or null; "default", which has no record, is not here.
pub fn meta(read: ReadView, name: []const u8) ?Meta.Value {
    if (name.len == 0 or name.len > MAX_NAME_LEN) return null;
    var kb: [1 + MAX_NAME_LEN]u8 = undefined;
    const got = read.get(NameFam, nameKey(&kb, name)) catch unreachable; // written by this machine alone
    return if (got) |s| s.v else null;
}

// ─── Apply ───────────────────────────────────────────────────────────────────

/// Why a create of `name` as `id` can't apply over `c` (with `exists`
/// saying whether the name has a record), or `.created`.
pub const Verdict = enum { created, existed, invalid, full, stale_id };

pub fn verdict(c: Counts.Value, exists: bool, name: []const u8, id: u32) Verdict {
    if (exists or isDefault(name)) return .existed;
    if (nameRefusal(name) != null) return .invalid;
    if (c.live >= MAX_LIVE) return .full;
    // The id must be the next free one or past it, so replicas agree
    // whatever their leader thought; one below it lost a race after a
    // leader change.
    if (id < c.next_id or id >= ns_table.CAPACITY) return .stale_id;
    return .created;
}

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    if (typeOf(e) != .namespace_create) return;
    var diag: schema.Diagnostic = .{};
    const c = namespace_entries.Create.decodeAll(e.payload, &diag) catch return;
    try out.add(NameFam.ID, "", c.name);
    try out.add(CountsFam.ID, "", COUNTS_KEY);
}

/// A create a ready leader proposed is never refused (`proposeCreate`
/// checks the name, the room and the id against everything applied and in
/// flight), so one that is refused here breaks the log: the writes behind
/// it carry an id that isn't theirs. It halts, on every replica alike.
fn apply(view: *View, e: *const Entry, _: *Result) ApplyError!void {
    if (typeOf(e) != .namespace_create) return error.Halt;
    // Only shard 0 logs creates; one elsewhere was never proposed. Every
    // shard's store has a place; only a test's may have none, and is then
    // taken as shard 0's.
    if (view.place) |p| if (p.group != 0) return error.Halt;
    var diag: schema.Diagnostic = .{};
    const c = namespace_entries.Create.decodeAll(e.payload, &diag) catch return error.Halt;
    var kb: [1 + MAX_NAME_LEN]u8 = undefined;
    // An invalid name may be longer than a key's buffer: refused unread.
    if (nameRefusal(c.name) != null) return error.Halt;
    const name_key = nameKey(&kb, c.name);
    var ckb: [1 + COUNTS_KEY.len]u8 = undefined;
    const counts_key = countsKey(&ckb);
    const now: Counts.Value = if (try view.get(CountsFam, counts_key)) |s| s.v else INITIAL;
    const exists = try view.get(NameFam, name_key) != null;
    if (verdict(now, exists, c.name, c.id) != .created) return error.Halt;
    try view.put(NameFam, name_key, .{ .v = .{ .id = c.id, .created_at_ns = e.header.timestamp_ns, .created_index = e.header.index } });
    try view.put(CountsFam, counts_key, .{ .v = .{ .next_id = c.id + 1, .live = now.live + 1 } });
}

pub const machine: machine_mod.Machine = .{
    .name = "namespaces",
    .entry_types = &.{.namespace_create},
    .families = &.{ machine_mod.info(NameFam), machine_mod.info(CountsFam) },
    .keys = keys,
    .apply = apply,
};

// ─── Tests ───────────────────────────────────────────────────────────────────

const testing = std.testing;
const Store = @import("../contract/store.zig").Store;

const Fixture = struct {
    store: Store,
    scratch: std.heap.ArenaAllocator,

    fn init(group: u16) Fixture {
        var f: Fixture = .{ .store = Store.init(testing.allocator), .scratch = .init(testing.allocator) };
        f.store.place = .{ .group = group, .count = group + 1 };
        return f;
    }

    fn deinit(f: *Fixture) void {
        f.store.deinit();
        f.scratch.deinit();
    }

    fn create(f: *Fixture, index: u64, name: []const u8, id: u32) !void {
        var buf: [namespace_entries.Create.max_size + 64]u8 = undefined;
        const bytes = namespace_entries.Create.encodeInto(&buf, .{ .name = name, .id = id }) catch unreachable;
        const e = entry_mod.buildEntry(.namespace_create, 0, 1, index, index * 1000, bytes);
        var out = Result.start(&e);
        try machine_mod.applyEntry(&machine, &f.store, &f.scratch, &e, &out);
        try testing.expect(out.refused() == null);
    }

    fn read(f: *Fixture) ReadView {
        return .{ .store = &f.store };
    }
};

test "namespaces machine: a create stores its name, id, stamp and index, and moves the next id past it" {
    var f = Fixture.init(0);
    defer f.deinit();
    const first = ns_table.FIRST_ALLOCATED.int();
    try testing.expectEqual(INITIAL, counts(f.read()));
    try f.create(7, "orders", first);
    const m = meta(f.read(), "orders").?;
    try testing.expectEqual(first, m.id);
    try testing.expectEqual(@as(u64, 7000), m.created_at_ns);
    try testing.expectEqual(@as(u64, 7), m.created_index);
    try testing.expectEqual(Counts.Value{ .next_id = first + 1, .live = 2 }, counts(f.read()));
    // An id past the next free one is taken as given; the room counts
    // namespaces, not ids.
    try f.create(8, "ledger", first + 5);
    try testing.expectEqual(Counts.Value{ .next_id = first + 6, .live = 3 }, counts(f.read()));
}

test "namespaces machine: a create its leader could never have proposed halts and writes nothing" {
    var f = Fixture.init(0);
    defer f.deinit();
    const first = ns_table.FIRST_ALLOCATED.int();
    try f.create(1, "orders", first);
    const before = counts(f.read());
    // The name again, "default", a bad name, a taken id, a reserved id,
    // an id past the table.
    try testing.expectError(error.Halt, f.create(2, "orders", first + 1));
    try testing.expectError(error.Halt, f.create(3, "default", first + 1));
    try testing.expectError(error.Halt, f.create(4, "_flo", first + 1));
    try testing.expectError(error.Halt, f.create(5, "late", first));
    try testing.expectError(error.Halt, f.create(6, "sneaky", ns_table.SYSTEM.int()));
    try testing.expectError(error.Halt, f.create(7, "far", ns_table.CAPACITY));
    try testing.expectEqual(before, counts(f.read()));
    try testing.expect(meta(f.read(), "late") == null);
    // Only shard 0 logs creates.
    var other = Fixture.init(1);
    defer other.deinit();
    try testing.expectError(error.Halt, other.create(1, "x", first));
    try testing.expect(meta(other.read(), "x") == null);
}

test "namespaces machine: the room is the live count, and a full registry halts a create" {
    var f = Fixture.init(0);
    defer f.deinit();
    var name_buf: [16]u8 = undefined;
    const first = ns_table.FIRST_ALLOCATED.int();
    // "default" holds one place from the start.
    for (0..MAX_LIVE - 1) |i| try f.create(i + 1, try std.fmt.bufPrint(&name_buf, "ns{d}", .{i}), first + @as(u32, @intCast(i)));
    try testing.expectEqual(MAX_LIVE, counts(f.read()).live);
    try testing.expectEqual(Verdict.full, verdict(counts(f.read()), false, "one-more", counts(f.read()).next_id));
    try testing.expectError(error.Halt, f.create(MAX_LIVE, "one-more", counts(f.read()).next_id));
}
