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
const proto = @import("../protocol/proto.zig");
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

/// A namespace's settings, set by an alter: how its run-history quota is
/// set (the default, none, or `bytes` over every shard), and the log index
/// that set it, which shard 0 publishes with it. An alter back to the
/// default keeps a row, so the index a namespace's quota is published
/// under only rises: a shard's stale-bind check holds across it, and across
/// a restart.
pub const QuotaMode = @import("../protocol/namespace_ops.zig").QuotaMode;
pub const Settings = Record(&.{
    .{ .name = "mode", .T = schema.Enum(QuotaMode) },
    .{ .name = "bytes", .T = Int(u64) },
    .{ .name = "set_index", .T = Int(u64) },
});

/// The quota a namespace's setting gives, over every shard: the default for
/// none set or one set back to it, `NO_QUOTA` for none.
pub fn quotaOf(s: ?Settings.Value) u64 {
    const set = s orelse return DEFAULT_HISTORY_QUOTA;
    return switch (set.mode) {
        .default => DEFAULT_HISTORY_QUOTA,
        .none => NO_QUOTA,
        .bytes => set.bytes,
    };
}

/// Whether a namespace's setting is its own, not the default.
pub fn quotaOwn(s: ?Settings.Value) bool {
    return if (s) |set| set.mode != .default else false;
}

pub const NameFam = view_mod.Family(4, "namespace.name", .shard, Stored(Meta));
pub const CountsFam = view_mod.Family(5, "namespace.meta", .shard, Stored(Counts));
/// Keyed by name, as `namespace.name` is: one only for a namespace an alter
/// named. One with none takes `DEFAULT_HISTORY_QUOTA`.
pub const SettingsFam = view_mod.Family(77, "namespace.settings", .shard, Stored(Settings));

/// A namespace's run-history quota until an alter sets its own, over every
/// shard. An input where it binds, not a logged value: changing it rebinds
/// every namespace with no setting of its own.
pub const DEFAULT_HISTORY_QUOTA: u64 = 10 << 30;

pub const NO_QUOTA = namespace_entries.NO_QUOTA;

/// The least a namespace's quota may be on one shard: one workflow run, or
/// one default action run, at the longest names. Below it every start
/// there would be refused for good. An alter is held to it per shard at
/// the op, where the node's shard count is known, and to it alone here.
pub const PART_FLOOR: u64 = @import("../budget/history_reserve.zig").PART_FLOOR;

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
/// `name`'s settings, or null for one no alter named.
pub fn settings(read: ReadView, name: []const u8) ?Settings.Value {
    if (name.len == 0 or name.len > MAX_NAME_LEN) return null;
    var kb: [1 + MAX_NAME_LEN]u8 = undefined;
    const got = read.get(SettingsFam, nameKey(&kb, name)) catch unreachable; // written by this machine alone
    return if (got) |s| s.v else null;
}

/// Why a quota set as `mode` with `bytes` can't be, held to `floor` bytes:
/// null if it can. Bytes come with the bytes mode and only with it.
pub fn quotaRefusal(mode: QuotaMode, bytes: ?u64, floor: u64) ?QuotaRefusal {
    if (mode != .bytes) return if (bytes == null) null else .malformed("bad request: a history quota's bytes come with its bytes mode only");
    const b = bytes orelse return .malformed("bad request: a history quota set in bytes needs its bytes");
    if (b == 0) return .outOfRange("bad request: a history quota of 0 would refuse every run; set none to turn it off");
    if (b == NO_QUOTA) return .outOfRange("bad request: a history quota is at most 2^64 - 2 bytes; set none to turn it off");
    if (b < floor) return .outOfRange("bad request: a history quota is at least one run's reserve on every shard");
    return null;
}

/// Why a quota is refused: a mode and bytes that don't go together is a
/// malformed request; a figure out of bounds is out of range.
pub const QuotaRefusal = struct {
    reason: proto.Reason,
    message: []const u8,

    fn malformed(m: []const u8) QuotaRefusal {
        return .{ .reason = .malformed, .message = m };
    }
    fn outOfRange(m: []const u8) QuotaRefusal {
        return .{ .reason = .out_of_range, .message = m };
    }
};

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
    if (typeOf(e) == .namespace_alter) {
        var d: schema.Diagnostic = .{};
        const a = namespace_entries.Alter.decodeAll(e.payload, &d) catch return;
        try out.add(NameFam.ID, "", a.name);
        try out.add(SettingsFam.ID, "", a.name);
        return;
    }
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
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    if (typeOf(e) == .namespace_alter) return applyAlter(view, e, out);
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

/// An alter of a namespace's settings, on shard 0: a well-formed entry is
/// refused, not halted on, for a name with no namespace or a quota below
/// one shard's floor, as its op refuses them before proposing; a quota per
/// shard below the floor on this node is the op's to refuse, since apply
/// reads no node config. One that doesn't decode (a mode no build writes)
/// halts: the op's schema refuses it before anything is proposed.
fn applyAlter(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    if (view.place) |p| if (p.group != 0) return error.Halt;
    var diag: schema.Diagnostic = .{};
    const a = namespace_entries.Alter.decodeAll(e.payload, &diag) catch return error.Halt;
    if (nameRefusal(a.name) != null and !isDefault(a.name)) return out.refuse(.bad_request, .malformed, "bad request: invalid namespace name");
    var kb: [1 + MAX_NAME_LEN]u8 = undefined;
    const k = nameKey(&kb, a.name);
    if (!isDefault(a.name) and try view.get(NameFam, k) == null) return out.refuse(.no_namespace, .not_found, "namespace not found");
    if (quotaRefusal(a.mode, if (a.mode == .bytes) a.bytes else if (a.bytes == 0) null else a.bytes, PART_FLOOR)) |why| return out.refuse(.bad_request, why.reason, why.message);
    try view.put(SettingsFam, k, .{ .v = .{ .mode = a.mode, .bytes = if (a.mode == .bytes) a.bytes else 0, .set_index = e.header.index } });
}

pub const machine: machine_mod.Machine = .{
    .name = "namespaces",
    .entry_types = &.{ .namespace_create, .namespace_alter },
    .families = &.{ machine_mod.info(NameFam), machine_mod.info(CountsFam), machine_mod.info(SettingsFam) },
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

fn alter(f: *Fixture, index: u64, name: []const u8, mode: QuotaMode, bytes: u64) !Result {
    var buf: [namespace_entries.Alter.max_size]u8 = undefined;
    const encoded = namespace_entries.Alter.encodeInto(&buf, .{ .name = name, .mode = mode, .bytes = bytes }) catch unreachable;
    const e = entry_mod.buildEntry(.namespace_alter, 0, 1, index, index * 1000, encoded);
    var out = Result.start(&e);
    try machine_mod.applyEntry(&machine, &f.store, &f.scratch, &e, &out);
    return out;
}

test "namespaces machine: an alter sets a namespace's quota with its index, default's too; none and back to the default keep a row; a namespace not there, a quota of 0, below the floor, or bytes with another mode is refused and sets nothing" {
    var f = Fixture.init(0);
    defer f.deinit();
    try f.create(1, "orders", ns_table.FIRST_ALLOCATED.int());
    try testing.expect((try alter(&f, 2, "orders", .bytes, 20 << 30)).refused() == null);
    try testing.expectEqual(Settings.Value{ .mode = .bytes, .bytes = 20 << 30, .set_index = 2 }, settings(f.read(), "orders").?);
    try testing.expectEqual(@as(u64, 20 << 30), quotaOf(settings(f.read(), "orders")));
    try testing.expect((try alter(&f, 3, "default", .none, 0)).refused() == null);
    try testing.expectEqual(NO_QUOTA, quotaOf(settings(f.read(), "default")));
    // Back to the default: its own no longer, its index risen.
    try testing.expect((try alter(&f, 4, "orders", .default, 0)).refused() == null);
    try testing.expectEqual(Settings.Value{ .mode = .default, .bytes = 0, .set_index = 4 }, settings(f.read(), "orders").?);
    try testing.expectEqual(DEFAULT_HISTORY_QUOTA, quotaOf(settings(f.read(), "orders")));
    try testing.expect(!quotaOwn(settings(f.read(), "orders")));
    // A mode and bytes that don't go together is malformed; a figure out
    // of bounds is out of range.
    for ([_]struct { []const u8, QuotaMode, u64, proto.Reason }{
        .{ "never", .bytes, 20 << 30, .not_found },
        .{ "orders", .bytes, 0, .out_of_range },
        .{ "orders", .bytes, PART_FLOOR - 1, .out_of_range },
        .{ "orders", .none, 20 << 30, .malformed },
        .{ "orders", .default, 20 << 30, .malformed },
        .{ "orders", .bytes, NO_QUOTA, .out_of_range },
    }) |c| {
        const out = try alter(&f, 5, c[0], c[1], c[2]);
        try testing.expectEqual(c[3], out.refused().?.refusal.reason);
    }
    try testing.expectEqual(proto.Reason.malformed, quotaRefusal(.bytes, null, PART_FLOOR).?.reason);
    try testing.expectEqual(@as(u64, 4), settings(f.read(), "orders").?.set_index);
    try testing.expect(settings(f.read(), "never") == null);
    // Only shard 0 logs alters.
    var other = Fixture.init(1);
    defer other.deinit();
    try testing.expectError(error.Halt, alter(&other, 1, "default", .none, 0));
}
