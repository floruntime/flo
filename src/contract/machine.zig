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
};

/// What's wrong with a list of machines, or null: one owner per entry type,
/// one family per id and per name.
pub fn listRule(comptime machines: []const Machine) ?[]const u8 {
    for (machines, 0..) |m, i| {
        for (machines[0..i]) |earlier| {
            for (m.entry_types) |t| for (earlier.entry_types) |u| if (t == u)
                return std.fmt.comptimePrint("entry type {s} is owned by both {s} and {s}", .{ @tagName(t), earlier.name, m.name });
        }
        for (m.entry_types, 0..) |t, j| for (m.entry_types[0..j]) |u| if (t == u)
            return std.fmt.comptimePrint("{s} lists entry type {s} twice", .{ m.name, @tagName(t) });
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
    _ = scratch.reset(.retain_capacity);
    const arena = scratch.allocator();
    var keys = view_mod.KeySet.init(arena);
    try m.keys(e, &keys);
    var view = view_mod.View.init(store, arena, &keys);
    try m.apply(&view, e, out);
    if (out.refused() != null) return;
    try view.commit();
}
