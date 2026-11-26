//! A namespace's run-history quota at admission, on one shard: its usage
//! (`history.usage`), what its live runs may still add (their reserves,
//! `workflow.reserved` and `action.reserved`), and its bound part
//! (`settings.quota`).
//!
//! A run reserves, at admission, the most its history can still grow once
//! its namespace's usage reaches the part (`history_reserve.zig`), and
//! releases all of it at its end. An admission is refused when the usage
//! has reached the part, or when the usage and every live run's reserve,
//! its own staged first, would pass it. Each reserve family is written by
//! its runs' machine alone; every arm reads only replicated rows, so every
//! replica reaches the same verdict.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const settings = @import("../settings/machine.zig");
const hm = @import("../stream/history_machine.zig");
const channels = @import("../contract/channels.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Int = schema.Int;

pub const Reserved = schema.Record(&.{
    .{ .name = "bytes", .T = Int(u64) },
});

/// One row per namespace on a shard: the reserves of its live workflow
/// runs, written by the runs machine alone.
pub const WorkflowReserved = view_mod.Family(79, "workflow.reserved", .id, delivery.Stored(Reserved));
/// The same for its live action runs, written by the action machine alone.
pub const ActionReserved = view_mod.Family(80, "action.reserved", .id, delivery.Stored(Reserved));

/// The key an admission or a release of namespace `ns` reads and writes in
/// `F`, the family its machine owns. The other machine's reserve, the usage
/// and the part are read from the committed store and declare nothing: one
/// inbound's admissions are all one machine's, so only its own family
/// stages between them.
pub fn addKeys(comptime F: type, out: *KeySet, ns: NamespaceId) error{OutOfMemory}!void {
    try out.addIn(F.ID, ns, "");
}

fn key(buf: *[4]u8, ns: NamespaceId) []const u8 {
    return view_mod.idKey(buf, ns, "") catch unreachable;
}

/// `ns`'s reserve in `F`, as staged in `view`.
pub fn reservedIn(comptime F: type, view: *View, ns: NamespaceId) persistence.ApplyError!u64 {
    var kb: [4]u8 = undefined;
    return if (try view.get(F, key(&kb, ns))) |r| r.v.bytes else 0;
}

/// `ns`'s reserve in `F`, as committed.
pub fn reservedOf(comptime F: type, read: ReadView, ns: NamespaceId) u64 {
    var kb: [4]u8 = undefined;
    const got = read.get(F, key(&kb, ns)) catch null;
    return if (got) |r| r.v.bytes else 0;
}

/// Raise `ns`'s reserve in `F` by `bytes`, saturating.
pub fn raise(comptime F: type, view: *View, ns: NamespaceId, bytes: u64) persistence.ApplyError!void {
    if (bytes == 0) return;
    var kb: [4]u8 = undefined;
    const k = key(&kb, ns);
    const have: u64 = if (try view.get(F, k)) |r| r.v.bytes else 0;
    try view.put(F, k, .{ .v = .{ .bytes = have +| bytes } });
}

/// Lower `ns`'s reserve in `F` by `bytes`. A lower past 0 is a Flo bug a
/// VOPR rule finds: saturated and counted (`View.quota_reserve_drift`),
/// never a halt, which every replica would hit again on replay. A row
/// lowered to 0 is deleted.
pub fn lower(comptime F: type, view: *View, ns: NamespaceId, bytes: u64) persistence.ApplyError!void {
    if (bytes == 0) return;
    var kb: [4]u8 = undefined;
    const k = key(&kb, ns);
    const have: u64 = if (try view.get(F, k)) |r| r.v.bytes else 0;
    if (bytes > have) view.quota_reserve_drift += 1;
    const left = have -| bytes;
    if (left == 0) {
        if (have > 0) try view.delete(F, k);
        return;
    }
    try view.put(F, k, .{ .v = .{ .bytes = left } });
}

/// What the quota weighed: the usage, every live run's reserve (the
/// admission's own included), the part, and that admission's own reserve.
pub const Over = struct { use: u64, reserved: u64, part: u64, own: u64 };

pub const Arm = enum {
    /// Refused once the usage has reached the part: a signal's arm.
    usage,
    /// That, then refused when the usage and the reserves pass the part.
    usage_and_reserve,
};

/// Null when `ns`'s part on this shard admits what `view` stages, its own
/// reserve `own` raised already in `Own`, its machine's family; otherwise
/// the figures it would pass. No part bound (before the first bind) or none
/// admits everything.
pub fn verdict(comptime Own: type, view: *View, ns: NamespaceId, own: u64, arm: Arm) persistence.ApplyError!?Over {
    const Other = if (Own == WorkflowReserved) ActionReserved else WorkflowReserved;
    const read: ReadView = .{ .store = view.store };
    const part = (settings.boundQuota(read, ns) orelse return null).part_bytes;
    if (part == settings.NO_QUOTA) return null;
    const use = inFlightIn(view, ns);
    const reserved = (try reservedIn(Own, view, ns)) +| reservedOf(Other, read, ns);
    const over: Over = .{ .use = use, .reserved = reserved, .part = part, .own = own };
    if (use >= part) return over;
    if (arm == .usage_and_reserve and use +| reserved > part) return over;
    return null;
}

/// A leader's check before it proposes a start or a child of `ns` that
/// would reserve `own`: the same rule as `verdict`, from the committed
/// store alone. It can race a chunk landing between the read and the
/// apply, so the apply's refusal stays handled.
pub fn preCheck(read: ReadView, ns: NamespaceId, own: u64) ?Over {
    const part = (settings.boundQuota(read, ns) orelse return null).part_bytes;
    if (part == settings.NO_QUOTA) return null;
    const use = inFlight(read, ns);
    const reserved = reservedOf(WorkflowReserved, read, ns) +| reservedOf(ActionReserved, read, ns) +| own;
    const over: Over = .{ .use = use, .reserved = reserved, .part = part, .own = own };
    if (use >= part or use +| reserved > part) return over;
    return null;
}

/// Whether `ns`'s in-flight history has reached its part, as committed: a
/// non-waking signal's arm, for a model to predict, and what holds a
/// namespace's runs' steps (the step hold).
///
/// A bare compare, with no hysteresis: a hold at it doesn't flap only
/// because `inFlight` doesn't move when a chunk lands or is trimmed
/// (`channels.NsBytes.delivered`). A change to what `inFlight` counts must
/// keep that, or holds start and clear with every land. What lowers it (a
/// lost chunk, a fold left unread or skipped, a drop) and a larger bound
/// part mark the namespace eased (`Store.Eased`), which is what asks its
/// holds again.
pub fn atUsage(read: ReadView, ns: NamespaceId) bool {
    const part = (settings.boundQuota(read, ns) orelse return false).part_bytes;
    if (part == settings.NO_QUOTA) return false;
    return inFlight(read, ns) >= part;
}

/// `ns`'s history on this shard as committed, landed or on its way: its
/// usage and what of its own history-lane bytes no inbound has taken. Its
/// own namespace's alone: never the lane's total, nor its cap.
pub fn inFlight(read: ReadView, ns: NamespaceId) u64 {
    const own: u16 = if (read.store.place) |pl| pl.group else 0;
    return hm.usageOf(read, ns).bytes +| channels.nsUntakenOf(read, own, ns);
}

/// `inFlight` as this apply sees it, as `historyHeld` sums a lane: the
/// committed figure, which a land earlier in the apply leaves where it was,
/// with the sends it holds for `ns` (origin `ns`, on the history lane). An
/// earlier message's sends in the same inbound are already in the outbox's
/// staged rows, which this doesn't read: so it may count up to one inbound's
/// history short, and admit, never hold, on it. That is the one step in
/// flight the workflow reserve counts.
fn inFlightIn(view: *const View, ns: NamespaceId) u64 {
    const read: ReadView = .{ .store = view.store };
    var n = inFlight(read, ns);
    for (view.sends.items) |snd| if (snd.flags & entry_mod.Flags.HISTORY != 0 and snd.origin.ns == ns) {
        n +|= snd.payload.len;
    };
    return n;
}

/// The refusal for an admission `o` holds back, under 128 bytes, from the
/// namespace's own id and figures alone. A run whose own reserve is past
/// the part is told so: deleting runs can't admit it, and a smaller run or
/// a larger quota can.
pub fn refuse(out: *persistence.Result, view: *const View, ns: NamespaceId, o: Over) void {
    // Counted apart from the memory refusals (`flo_shard_quota_refusals_total`).
    view.store.quota_refusals += 1;
    const shard: u16 = if (view.store.place) |pl| pl.group else 0;
    // A refusal answers with its reason alone.
    out.len = 0;
    out.overflowed = false;
    var buf: [128]u8 = undefined;
    out.refuseFmt(.limit, .history_quota, "{s}", .{why(&buf, ns, shard, o)});
}

/// What `refuse` would say, for a parent's call or a log line.
pub fn why(buf: []u8, ns: NamespaceId, shard: u16, o: Over) []const u8 {
    if (o.own > o.part)
        return std.fmt.bufPrint(buf, "limit: ns {d} run reserve {d} over part {d} on shard {d}", .{ ns.int(), o.own, o.part, shard }) catch "limit: run reserve over the part";
    return std.fmt.bufPrint(buf, "limit: ns {d} history {d}+{d} of {d} on shard {d}", .{ ns.int(), o.use, o.reserved, o.part, shard }) catch "limit: run history quota";
}

/// Why `F`'s rows aren't `sums`, each namespace's live runs' reserves as
/// their machine computes them, or null: a row per namespace with a
/// reserve, holding exactly it, and none for a namespace with none. A
/// VOPR rule; walks the family.
pub fn ruleAgainst(comptime F: type, read: ReadView, sums: *const std.AutoHashMapUnmanaged(u32, u64)) ?[]const u8 {
    var seen: usize = 0;
    var it = read.range(F, "");
    while (it.next() catch return "a reserve row could not be read") |item| {
        if (item.key.len != 4) return "a reserve row's key is not a namespace";
        const want = sums.get(std.mem.readInt(u32, item.key[0..4], .big)) orelse return "a namespace with no live runs has a reserve row";
        if (item.value.v.bytes != want) return "a namespace's reserve is not the sum of its live runs' reserves";
        seen += 1;
    }
    var nonzero: usize = 0;
    var vit = sums.valueIterator();
    while (vit.next()) |v| nonzero += @intFromBool(v.* != 0);
    if (nonzero != seen) return "a namespace's live runs reserve bytes it has no reserve row for";
    return null;
}

/// A test's store with `ns`'s part bound to `part` and its usage set to
/// `use`, as a bind and the history machine would leave them.
pub fn setForTest(store: *@import("../contract/store.zig").Store, ns: NamespaceId, part: u64, use: u64) !void {
    var kb: [4]u8 = undefined;
    const k = key(&kb, ns);
    var qb: [settings.QuotaPart.max_size]u8 = undefined;
    try putRaw(store, settings.QuotaFam.ID, k, try settings.QuotaPart.encodeInto(&qb, .{ .part_bytes = part, .published_index = 1 }));
    var ub: [hm.Usage.max_size]u8 = undefined;
    try putRaw(store, hm.UsageFam.ID, k, try hm.Usage.encodeInto(&ub, .{ .bytes = use, .orphans = 0, .orphan_bytes = 0 }));
}

/// A test's store with `ns`'s reserve in `F` set to `bytes`, as its machine
/// would leave it.
pub fn setReservedForTest(comptime F: type, store: *@import("../contract/store.zig").Store, ns: NamespaceId, bytes: u64) !void {
    var kb: [4]u8 = undefined;
    var rb: [Reserved.max_size]u8 = undefined;
    try putRaw(store, F.ID, key(&kb, ns), try Reserved.encodeInto(&rb, .{ .bytes = bytes }));
}

fn putRaw(store: *@import("../contract/store.zig").Store, fam: u16, k: []const u8, bytes: []const u8) !void {
    if (store.find(fam, k)) |node| {
        store.replace(fam, node, try store.allocator.dupe(u8, bytes));
    } else store.link(fam, try store.allocRecord(k), try store.allocator.dupe(u8, bytes));
}

const testing = std.testing;

test "history quota: a leader's check sums both machines' reserves and its own against the part" {
    var store = @import("../contract/store.zig").Store.init(testing.allocator);
    defer store.deinit();
    const read: ReadView = .{ .store = &store };
    try setForTest(&store, .default, 1000, 100);
    try testing.expect(preCheck(read, .default, 900) == null);
    // Each machine's reserve alone takes it over.
    try setReservedForTest(ActionReserved, &store, .default, 1);
    try testing.expectEqual(@as(u64, 1001), preCheck(read, .default, 900).?.use + preCheck(read, .default, 900).?.reserved);
    try setReservedForTest(ActionReserved, &store, .default, 0);
    try setReservedForTest(WorkflowReserved, &store, .default, 1);
    try testing.expect(preCheck(read, .default, 900) != null);
    // The usage at the part, with nothing reserved.
    try setReservedForTest(WorkflowReserved, &store, .default, 0);
    try setForTest(&store, .default, 1000, 1000);
    try testing.expect(preCheck(read, .default, 0) != null);
    try testing.expect(atUsage(read, .default));
}

test "history quota: the refusal names the namespace's own figures in under 128 bytes, and a run past its part says so" {
    var buf: [128]u8 = undefined;
    const most = std.math.maxInt(u64);
    const at: NamespaceId = .of(4095);
    const w = why(&buf, at, 65535, .{ .use = most, .reserved = most, .part = most, .own = 1 });
    try testing.expect(w.len < 128);
    try testing.expect(std.mem.startsWith(u8, w, "limit: ns 4095 history "));
    const r = why(&buf, at, 65535, .{ .use = 0, .reserved = most, .part = most - 1, .own = most });
    try testing.expect(r.len < 128);
    try testing.expect(std.mem.startsWith(u8, r, "limit: ns 4095 run reserve "));
}
