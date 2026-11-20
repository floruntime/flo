//! The per-shard memory pool's running use: one `.aggregate` record holding
//! the resident bytes every namespace on the shard keeps across the data
//! families (kv, queue, group, stream, ts, actions, and the metadata
//! creates). FLO-174 PR 3.
//!
//! This machine owns the `budget.pool_use` family but applies no entry of
//! its own. Each charging family declares `Uses.ID` in its `keys()` and
//! charges or refunds through `charge`/`refund` here, inside its own apply,
//! at the point the resident write happens — so the total lives in one place
//! every replica recomputes identically at replay (a record is undone by
//! rollback and rebuilt by replay; a Shard field would be skipped). The
//! total is `.aggregate`, exactly like `queue.held`: any namespace's message
//! may move it, since the charging apply checks the total itself.
//!
//! Enforcement (a client write refused when `use + delta` tops the shard's
//! bound budget) reads this record; it is wired in the charging applies, not
//! here. Observability-only until that wiring lands.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const Entry = entry_mod.Entry;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Record = schema.Record;
const Stored = delivery.Stored;

/// The shard's resident pool use, in bytes. One record, under `USE_KEY`.
pub const Use = Record(&.{
    .{ .name = "bytes", .T = Int(u64) },
});

pub const Uses = view_mod.Family(67, "budget.pool_use", .aggregate, Stored(Use));

/// `budget.pool_use`'s one key, in no namespace: a zero-length namespace
/// prefix (`\x00`) then the bare key, the composite form `view.get`/`put`
/// take (mirrors `queue.held`'s `HELD_KEY`).
pub const USE_KEY = "\x00use";

/// Add `(Uses.ID, "", "use")` to an entry's key set. A charging machine's
/// `keys()` calls this for any entry type it charges or refunds, so its
/// apply is allowed to write the shared record.
pub fn declareKey(out: *KeySet) error{OutOfMemory}!void {
    try out.add(Uses.ID, "", USE_KEY[1..]);
}

/// The shard's resident pool use in bytes (0 when no record exists yet).
pub fn useOf(view: *View) ApplyError!u64 {
    return if (try view.get(Uses, USE_KEY)) |u| u.v.bytes else 0;
}

/// Add `size` resident bytes to the pool.
pub fn charge(view: *View, size: u64) ApplyError!void {
    if (size == 0) return;
    const use = try useOf(view);
    try view.put(Uses, USE_KEY, .{ .v = .{ .bytes = use + size } });
}

/// Return `size` resident bytes to the pool. An underflow means a
/// charge/refund miscount (a byte refunded twice, or freed without a
/// matching charge): deterministic across replicas, so it halts rather than
/// saturating — a wedged pool is a detectable bug, a silently-clamped total
/// hides one. The VOPR ledger invariant (`use == Σ live resident bytes`)
/// catches a mismatch before it reaches here.
pub fn refund(view: *View, size: u64) ApplyError!void {
    if (size == 0) return;
    const use = try useOf(view);
    if (size > use) return error.Halt;
    try view.put(Uses, USE_KEY, .{ .v = .{ .bytes = use - size } });
}

// ─── Machine: owns the family, applies nothing of its own ──────────────────

fn keys(_: *const Entry, _: *KeySet) error{OutOfMemory}!void {}

fn apply(_: *View, _: *const Entry, _: *Result) ApplyError!void {}

pub const machine: machine_mod.Machine = .{
    .name = "budget",
    .entry_types = &.{},
    .families = &.{machine_mod.info(Uses)},
    .keys = keys,
    .apply = apply,
};

test "charge and refund move the pool use, underflow halts" {
    const testing = std.testing;
    const store_mod = @import("../contract/store.zig");
    var store = store_mod.Store.init(testing.allocator);
    store.strict_keys = true;
    defer store.deinit();
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();

    // A charging apply builds its view the way `applyEntry` does: a key set
    // that declares the shared record, then the view over it.
    const run = struct {
        fn with(st: *store_mod.Store, scr: *std.heap.ArenaAllocator, comptime body: fn (*View) anyerror!void) anyerror!void {
            _ = scr.reset(.retain_capacity);
            const arena = scr.allocator();
            var keys_set = view_mod.KeySet.init(arena);
            try declareKey(&keys_set);
            var view = view_mod.View.init(st, arena, &keys_set);
            try body(&view);
            try view.commit();
        }
    };

    try run.with(&store, &scratch, struct {
        fn f(v: *View) anyerror!void {
            try testing.expectEqual(@as(u64, 0), try useOf(v));
            try charge(v, 100);
            try charge(v, 25);
            try refund(v, 40);
            try testing.expectEqual(@as(u64, 85), try useOf(v));
        }
    }.f);

    try run.with(&store, &scratch, struct {
        fn f(v: *View) anyerror!void {
            try testing.expectEqual(@as(u64, 85), try useOf(v));
            try testing.expectError(error.Halt, refund(v, 86));
        }
    }.f);
}
