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

const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;
const ApplyError = persistence.ApplyError;

pub const FamilyInfo = struct { id: u16, name: []const u8 };

pub fn info(comptime F: type) FamilyInfo {
    return .{ .id = F.ID, .name = F.NAME };
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
    /// unbounded walk: larger work goes to entries of its own.
    observes: []const EntryType = &.{},
    observe_keys: ?*const fn (e: *const Entry, out: *view_mod.KeySet) error{OutOfMemory}!void = null,
    observe: ?*const fn (view: *view_mod.View, e: *const Entry) ApplyError!void = null,
};

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
        for (m.observes) |t| for (machines[0..i]) |earlier| for (earlier.observes) |u| if (t == u)
            return std.fmt.comptimePrint("entry type {s} is observed by both {s} and {s}", .{ @tagName(t), earlier.name, m.name });
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

/// The machine that follows `etype` another applier owns, if one does.
pub fn observerOf(comptime machines: []const Machine, etype: EntryType) ?*const Machine {
    inline for (machines) |*m| {
        for (m.observes) |t| if (t == etype) return m;
    }
    return null;
}

/// `m`'s observe of `e`, an entry another applier applied: its writes become
/// state with it.
pub fn observeEntry(m: *const Machine, store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, streams: ?Streams, e: *const Entry) ApplyError!void {
    _ = scratch.reset(.retain_capacity);
    const arena = scratch.allocator();
    var keys = view_mod.KeySet.init(arena);
    try m.observe_keys.?(e, &keys);
    var view = view_mod.View.init(store, arena, &keys);
    view.streams_read = streams;
    try m.observe.?(&view, e);
    try view.commit();
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
    try view.commit();
}

test "listRule: an observer needs its observe, follows only entries no machine owns, and is the only one for its type" {
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
        std.debug.assert(std.mem.eql(u8, listRule(&.{ T.of("a", &.{}, &.{.stream_trim}, true), T.of("b", &.{}, &.{.stream_trim}, true) }).?, "entry type stream_trim is observed by both a and b"));
    }
}
