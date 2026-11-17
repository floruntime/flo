//! A state machine on the contract: the entry types it owns, the families
//! it keeps, which keys an entry may touch, and its apply, which reaches
//! state only through a `View`. `applyEntry` runs one entry through it:
//! the staged writes become state when the entry applies, and are dropped
//! when it is refused or halts.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("store.zig");
const view_mod = @import("view.zig");
const Streams = @import("streams.zig").Streams;
const channels = @import("channels.zig");

const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;
const ApplyError = persistence.ApplyError;

pub const FamilyInfo = struct { id: u16, name: []const u8, scope: view_mod.Scope };

pub fn info(comptime F: type) FamilyInfo {
    return .{ .id = F.ID, .name = F.NAME, .scope = F.SCOPE };
}

pub const Machine = struct {
    name: []const u8,
    /// Each entry type has one owner, here or elsewhere.
    entry_types: []const EntryType,
    /// In the order they encode.
    families: []const FamilyInfo,
    /// Every key the entry may touch, from the entry alone.
    keys: *const fn (e: *const Entry, out: *view_mod.KeySet) error{OutOfMemory}!void,
    /// Pure and total: the same entry over the same state gives the same
    /// writes and the same result on every replica. "Now" is the entry's
    /// stamp. A refusal is said on `out` and drops the staged writes.
    apply: *const fn (view: *view_mod.View, e: *const Entry, out: *persistence.Result) ApplyError!void,
    /// Entry types another applier owns that this machine follows: after one
    /// applies and isn't refused, `observe` runs over this machine's own
    /// families, in the same index order on every replica. It never refuses,
    /// and its work in one apply is bounded by what it keeps, never by an
    /// unbounded walk: larger work goes to entries of its own. Several
    /// machines may follow one type (`observeAll`).
    observes: []const EntryType = &.{},
    observe_keys: ?*const fn (e: *const Entry, out: *view_mod.KeySet) error{OutOfMemory}!void = null,
    observe: ?*const fn (view: *view_mod.View, e: *const Entry) ApplyError!void = null,
    /// Whether `observe` has anything to do for `e`, read from state alone,
    /// for a type followed often and seldom changed by (each append to a
    /// stream that exists): a no skips the view `observe` would build.
    /// Null runs `observe` always.
    observe_needed: ?*const fn (read: view_mod.ReadView, e: *const Entry) bool = null,
    /// For a target that can be full for a while (a queue at its caps): the
    /// target an entry writes, so a channel message it refuses `overloaded`
    /// is parked under it rather than lost, and later messages for it park
    /// behind (`channels.zig`). Null for an entry of another kind.
    target: ?*const fn (e: *const Entry) ?[TARGET_LEN]u8 = null,
    /// Whether the entry's target has room for it now, from state alone, as
    /// its apply would decide: what a leader checks before unparking.
    room: ?*const fn (read: view_mod.ReadView, e: *const Entry) bool = null,
    /// The target's name for an operator, from one entry that writes it
    /// ("queue orders"), into `buf`.
    label: ?*const fn (e: *const Entry, buf: []u8) []const u8 = null,
    /// The namespace id an entry's payload names, for one whose applier
    /// can't take an id no namespace could have: a channel message naming
    /// one is refused before its machine sees it (`channels.zig`).
    namespace: ?*const fn (e: *const Entry) ?u32 = null,
};

/// A target as its machine names it; channels tag it with the machine.
pub const TARGET_LEN = @import("../protocol/channel_entries.zig").MACHINE_TARGET_LEN;

/// What's wrong with a list of machines, or null: one owner per entry type,
/// one family per id and per name.
pub fn listRule(comptime machines: []const Machine) ?[]const u8 {
    @setEvalBranchQuota(100_000);
    for (machines, 0..) |m, i| {
        for (machines[0..i]) |earlier| {
            for (m.entry_types) |t| for (earlier.entry_types) |u| if (t == u)
                return std.fmt.comptimePrint("entry type {s} is owned by both {s} and {s}", .{ @tagName(t), earlier.name, m.name });
        }
        for (m.entry_types, 0..) |t, j| for (m.entry_types[0..j]) |u| if (t == u)
            return std.fmt.comptimePrint("{s} lists entry type {s} twice", .{ m.name, @tagName(t) });
        if (m.observes.len > 0 and (m.observe == null or m.observe_keys == null))
            return std.fmt.comptimePrint("{s} observes entries but has no observe", .{m.name});
        for (m.observes) |t| for (machines) |o| for (o.entry_types) |u| if (t == u)
            return std.fmt.comptimePrint("{s} observes {s}, which a machine owns: it applies it", .{ m.name, @tagName(t) });
        // Several machines may observe one type, under rules every replica
        // keeps alike: they run in this list's order (`observeAll`); each
        // writes only its own families (`observeEntry` halts otherwise); none
        // can refuse, as `observe` has no result; and each runs inside the
        // keys it declares itself, in a view of its own. A message applied
        // after its inbound commits is followed the same way; its observers
        // key by the namespace its payload names, which the channel holds to
        // the message's origin (`AfterCommit.namespace`).
        for (m.observes, 0..) |t, j| for (m.observes[0..j]) |u| if (t == u)
            return std.fmt.comptimePrint("{s} observes entry type {s} twice", .{ m.name, @tagName(t) });
    }
    var seen: []const FamilyInfo = &.{};
    for (machines) |m| for (m.families) |f| {
        if (f.id >= store_mod.MAX_FAMILIES) return std.fmt.comptimePrint("family {s} has id {d}, past {d}", .{ f.name, f.id, store_mod.MAX_FAMILIES });
        for (seen) |s| {
            if (s.id == f.id) return std.fmt.comptimePrint("families {s} and {s} share id {d}", .{ s.name, f.name, f.id });
            if (std.mem.eql(u8, s.name, f.name)) return std.fmt.comptimePrint("two families are named {s}", .{f.name});
        }
        seen = seen ++ .{f};
    };
    return null;
}

pub fn check(comptime machines: []const Machine) void {
    if (comptime listRule(machines)) |message| @compileError(message);
}

/// Every machine that follows `e`, an entry another applier applied and
/// didn't refuse, in the list's order: each commits before the next runs, so
/// a later one reads what an earlier one wrote, on every replica alike.
pub fn observeAll(comptime machines: []const Machine, store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, streams: ?Streams, e: *const Entry) ApplyError!void {
    const etype: EntryType = @fromBackingInt(@intCast(e.header.entry_type));
    inline for (machines) |*m| {
        for (m.observes) |t| if (t == etype) {
            try observeEntry(m, store, scratch, streams, e);
            break;
        };
    }
}

/// `m`'s observe of `e`, an entry another applier applied: its writes become
/// state with it, and what it sends leaves with them. A write to another
/// machine's family halts: an observer keeps only its own.
///
/// An observer sends only on observing an edge entry (a client's write, or
/// a leader's own proposal: a completion, a refusal), never because of a
/// message it applied. So a chain of applies never sends, and needs no fuel
/// or `hops` to bound it.
pub fn observeEntry(m: *const Machine, store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, streams: ?Streams, e: *const Entry) ApplyError!void {
    if (m.observe_needed) |needed| if (!needed(.{ .store = store }, e)) return;
    _ = scratch.reset(.retain_capacity);
    const arena = scratch.allocator();
    var keys = view_mod.KeySet.init(arena);
    try m.observe_keys.?(e, &keys);
    var view = view_mod.View.init(store, arena, &keys);
    view.streams_read = streams;
    try m.observe.?(&view, e);
    for (view.ops.items) |op| if (!ownsFamily(m, op.fam)) return error.Halt;
    if (view.sends.items.len > 0) {
        // An observed entry is never a message, so it is no channel's.
        if (e.header.entry_type == @backingInt(EntryType.channel_inbound)) return error.Halt;
        if (channels.tooMuchSent(view.sends.items) != null) return error.Halt;
        try channels.writeSends(&view, e.header.index);
    }
    try view.commit();
}

fn ownsFamily(m: *const Machine, fam: u16) bool {
    for (m.families) |f| if (f.id == fam) return true;
    return false;
}

/// The machine that owns `etype`, if one does.
pub fn ownerOf(comptime machines: []const Machine, etype: EntryType) ?*const Machine {
    inline for (machines) |*m| {
        for (m.entry_types) |t| if (t == etype) return m;
    }
    return null;
}

/// Every family of `machines`, in the order they encode.
pub fn families(comptime machines: []const Machine) []const FamilyInfo {
    comptime {
        var all: []const FamilyInfo = &.{};
        for (machines) |m| all = all ++ m.families;
        return all;
    }
}

/// One entry through `m`: its keys, its apply over a `View`, then commit,
/// unless it was refused or failed. `scratch` holds the entry's staged
/// bytes and is reset first.
pub fn applyEntry(m: *const Machine, store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, e: *const Entry, out: *persistence.Result) ApplyError!void {
    return applyEntryWith(m, store, scratch, null, e, out);
}

/// `applyEntry`, with the shard's streams readable through the view.
pub fn applyEntryWith(m: *const Machine, store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, streams: ?Streams, e: *const Entry, out: *persistence.Result) ApplyError!void {
    _ = scratch.reset(.retain_capacity);
    const arena = scratch.allocator();
    var keys = view_mod.KeySet.init(arena);
    try m.keys(e, &keys);
    var view = view_mod.View.init(store, arena, &keys);
    view.streams_read = streams;
    try m.apply(&view, e, out);
    if (out.refused() != null) return;
    // What it sends leaves with its writes: the outbox is written in the
    // same commit, under this entry's index.
    if (view.sends.items.len > 0) {
        if (channels.tooMuchSent(view.sends.items)) |why| return out.refuse(.bad_request, .malformed, why);
        try channels.writeSends(&view, e.header.index);
    }
    try view.commit();
}

test "listRule: an observer needs its observe, follows only entries no machine owns, and names a type once" {
    const T = struct {
        fn keys(_: *const Entry, _: *view_mod.KeySet) error{OutOfMemory}!void {}
        fn apply(_: *view_mod.View, _: *const Entry, _: *persistence.Result) ApplyError!void {}
        fn observe(_: *view_mod.View, _: *const Entry) ApplyError!void {}
        fn of(name: []const u8, owns: []const EntryType, follows: []const EntryType, with_observe: bool) Machine {
            return .{ .name = name, .entry_types = owns, .families = &.{}, .keys = keys, .apply = apply, .observes = follows, .observe_keys = if (with_observe) keys else null, .observe = if (with_observe) observe else null };
        }
    };
    comptime {
        std.debug.assert(listRule(&.{T.of("a", &.{.cg_create}, &.{.stream_trim}, true)}) == null);
        std.debug.assert(std.mem.eql(u8, listRule(&.{T.of("a", &.{}, &.{.stream_trim}, false)}).?, "a observes entries but has no observe"));
        std.debug.assert(std.mem.eql(u8, listRule(&.{ T.of("a", &.{.cg_create}, &.{}, true), T.of("b", &.{}, &.{.cg_create}, true) }).?, "b observes cg_create, which a machine owns: it applies it"));
        std.debug.assert(listRule(&.{ T.of("a", &.{}, &.{.stream_trim}, true), T.of("b", &.{}, &.{.stream_trim}, true) }) == null);
        std.debug.assert(std.mem.eql(u8, listRule(&.{T.of("a", &.{}, &.{ .stream_trim, .stream_trim }, true)}).?, "a observes entry type stream_trim twice"));
    }
}
