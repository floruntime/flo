//! The memory gate's checker: every verdict the gate gives a top-level
//! entry, judged from the store alone, apart from the gate's own figures.
//!
//! A taken entry's charged bytes are what its commit moved the pool by; a
//! refused one's are what it would have moved it by, applied again on a
//! copy of the store with no share bound. Then:
//! - a refused entry was refusable, not a sink's, and would have passed the
//!   share;
//! - a taken refusable entry freed bytes or stayed within the share;
//! - nothing never refused is refused for memory;
//! - a group's deliver at its share takes no fresh record;
//! - a run's start is refused or held for memory only past the share, and
//!   one that made its run stayed within it, its sends apart (they are
//!   written after its verdict, `memory_budget.OWN_SENDS_BYTES`).

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const gate = @import("../budget/gate.zig");
const pool = @import("../budget/pool.zig");
const settings = @import("../settings/machine.zig");
const group_machine = @import("../stream/group_machine.zig");

const Store = store_mod.Store;
const Entry = entry_mod.Entry;

/// The first verdict found wrong since the last `take`, and the entry it was.
var broke: ?struct { why: []const u8, index: u64 } = null;
var checking = false;
var allocator: std.mem.Allocator = undefined;
/// Verdicts checked, refusals among them: a run that refused nothing
/// tested little.
pub var judged: u64 = 0;
pub var refused: u64 = 0;

pub fn enable(a: std.mem.Allocator) void {
    allocator = a;
    machine_mod.gate_watch = &WATCH;
}

/// The wrong verdict found since the last call, if any.
pub fn take() ?struct { why: []const u8, index: u64 } {
    defer broke = null;
    const b = broke orelse return null;
    return .{ .why = b.why, .index = b.index };
}

fn fail(e: *const Entry, why: []const u8) void {
    if (broke == null) broke = .{ .why = why, .index = e.header.index };
}

const WATCH: machine_mod.GateWatch = .{ .before = noteBefore, .after = watch };

/// The store's pool use and group items before the entry being applied.
var use_before: u64 = 0;
var data_before: u64 = 0;
var items_before: u64 = 0;

fn noteBefore(store: *Store) void {
    if (checking) return;
    use_before = pool.useOf(store);
    data_before = pool.dataUseOf(store);
    items_before = store.family(group_machine.Items.ID).count;
}

fn watch(m: *const machine_mod.Machine, store: *Store, e: *const Entry, out: *const persistence.Result) void {
    // A copy's own apply, below, is not a verdict.
    if (checking) return;
    const before = use_before;
    const share = (settings.boundShare(.{ .store = store }) orelse return).share_bytes;
    const t = std.enums.fromInt(entry_mod.EntryType, e.header.entry_type) orelse return;
    // Nothing a channel carries is refused for memory: a sink's message was
    // held before it was proposed, and the rest are the node's own.
    if (t == .channel_inbound or t == .channel_unpark) for (store.messages.items) |*msg| {
        if (msg.result.refused()) |r| if (r.refusal.reason == .memory_budget) return fail(e, "a channel's message was refused for memory");
    };
    const refusable = gate.refusable(e);
    const memory = if (out.refused()) |r| r.refusal.reason == .memory_budget else false;
    if (memory) refused += 1;
    if (refusable) judged += 1;
    if (t == .group_deliver) return judgeDeliver(store, e, memory, before, share);
    if (t == .workflow_start) return judgeStart(m, store, e, out, memory, before, share);
    if (memory and !refusable) return fail(e, "an entry never refused for memory was refused for it");
    if (!refusable) return;
    if (out.refused() != null and !memory) return;
    const delta: i64 = if (memory) wouldAdd(m, store, e, before) orelse return else @as(i64, @intCast(pool.useOf(store))) - @as(i64, @intCast(before));
    if (memory and gate.fits(before, delta, share)) return fail(e, "a write within its shard's share was refused for memory");
    if (!memory and !gate.fits(before, delta, share)) return fail(e, "a write past its shard's share was taken");
}

/// A deliver's own rule: at its share it takes no fresh record, and it is
/// refused for memory only there.
fn judgeDeliver(store: *Store, e: *const Entry, memory: bool, before: u64, share: u64) void {
    if (memory and before < share) return fail(e, "a group's deliver under its shard's share was refused for memory");
    if (memory or before < share) return;
    if (store.family(group_machine.Items.ID).count > items_before) return fail(e, "a group's deliver at its shard's share took a fresh record");
}

/// A start's own rule: refused for memory only when what it writes but its
/// sends would pass the share; taken, having made its run, only within it.
fn judgeStart(m: *const machine_mod.Machine, store: *Store, e: *const Entry, out: *const persistence.Result, memory: bool, before: u64, share: u64) void {
    judged += 1;
    if (memory) {
        const add = wouldAddData(m, store, e) orelse return;
        if (gate.fits(before, add, share)) return fail(e, "a run's start within its shard's share was refused for memory");
        return;
    }
    if (out.refused() != null) return;
    var r = out.reader();
    if ((r.int(u8) orelse 0) != 1) return;
    const add = @as(i64, @intCast(pool.dataUseOf(store))) - @as(i64, @intCast(data_before));
    if (!gate.fits(before, add, share)) return fail(e, "a run's start past its shard's share made its run");
}

/// What `e` would add to the charged families but the channels' of `store`,
/// applied on a copy of it with no share bound: what a run's start is judged
/// by, its sends written after. Null when the copy can't be made.
pub fn wouldAddData(m: *const machine_mod.Machine, store: *Store, e: *const Entry) ?i64 {
    const before = pool.dataUseOf(store);
    var copy = copyUnbound(store) orelse return null;
    defer copy.deinit();
    applyOnCopy(m, &copy, e) orelse return null;
    return @as(i64, @intCast(pool.dataUseOf(&copy))) - @as(i64, @intCast(before));
}

fn copyUnbound(store: *Store) ?Store {
    var copy = Store.init(allocator);
    copy.strict_keys = false;
    copy.place = store.place;
    for (0..store_mod.MAX_FAMILIES) |id| {
        if (id == settings.BindFam.ID) continue;
        var it = store.range(@intCast(id), "");
        while (it.next()) |rec| {
            const r = copy.allocRecord(rec.key()) catch {
                copy.deinit();
                return null;
            };
            copy.link(@intCast(id), r, allocator.dupe(u8, rec.value) catch {
                copy.deinit();
                return null;
            });
        }
    }
    return copy;
}

fn applyOnCopy(m: *const machine_mod.Machine, copy: *Store, e: *const Entry) ?void {
    checking = true;
    defer checking = false;
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    var out: persistence.Result = .{};
    out.reset(e);
    machine_mod.applyEntry(m, copy, &scratch, e, &out) catch return null;
}

/// What `e` would add to the pool of `store`, applied on a copy of it with
/// no share bound: null when the copy can't be made.
fn wouldAdd(m: *const machine_mod.Machine, store: *Store, e: *const Entry, before: u64) ?i64 {
    checking = true;
    defer checking = false;
    var copy = Store.init(allocator);
    defer copy.deinit();
    copy.strict_keys = false;
    copy.place = store.place;
    for (0..store_mod.MAX_FAMILIES) |id| {
        if (id == settings.BindFam.ID) continue;
        var it = store.range(@intCast(id), "");
        while (it.next()) |rec| {
            const r = copy.allocRecord(rec.key()) catch return null;
            copy.link(@intCast(id), r, allocator.dupe(u8, rec.value) catch return null);
        }
    }
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    var out: persistence.Result = .{};
    out.reset(e);
    machine_mod.applyEntry(m, &copy, &scratch, e, &out) catch return null;
    return @as(i64, @intCast(pool.useOf(&copy))) - @as(i64, @intCast(before));
}
