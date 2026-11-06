//! VOPR state families: toy state machines each replica applies, so the
//! checker can compare applied state, not only the log.
//!
//! A family's checksum is a hash of its state in a canonical order (keys
//! in key order, each value field by field), never the order its map
//! happens to hold them in. Two replicas that applied the same history
//! must agree on it, and so must a replica that rebuilt by replay after a
//! restart and one that applied the same entries live.
//!
//! `Kv` is the server's own KV machine on a store of its own, compared by
//! the contract's canonical checksum of each of its families. `Actions`
//! is the same for action runs, and also checks each entry it applies
//! against the state before and after it.
//!
//! `Namespaces` is shard 0's own namespace handler, its creates applied
//! through the handler's replay path.
//!
//! `Mutant` plants the bugs this check exists to catch, one at a time.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const kv_machine = @import("../kv/machine.zig");
const view_mod = @import("../contract/view.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const limits = @import("../protocol/limits.zig");
const schema = @import("../protocol/schema.zig");
const action_machine = @import("../actions/machine.zig");
const ae = @import("../protocol/action_entries.zig");
const ns_handler = @import("../namespace/handler.zig");
const namespace_entries = @import("../protocol/namespace_entries.zig");

const Allocator = std.mem.Allocator;
const HASH_SEED: u64 = 0xFA81_17E5;

pub const Mutant = enum {
    none,
    /// Apply reads the node's clock instead of the entry's stamp.
    node_clock,
    /// Whether an entry is refused depends on the replica (here, whether
    /// it leads), not on the entry and the state alone.
    local_refusal,
    /// The checksum walks the map in its own order. A replica rebuilt by
    /// replay reserves its map up front, as boot does, so its order
    /// differs from a replica that grew its map live.
    map_order,
    /// Part of an entry's effect runs only when it applies live (a
    /// post-apply hook that owns state), so replay rebuilds less.
    live_only_effect,
    /// A replica rebuilding by replay skips KV's batches.
    kv_replay_skips_batches,
    /// The contract commits a refused entry's staged writes, the same on
    /// every replica: no comparison sees it, so `Kv.apply` checks refusals
    /// itself.
    kv_refused_commits,
    /// A condition is decided by the replica: the leader writes what every
    /// other replica refuses.
    kv_condition_local,
    /// `if_absent` always holds, on every replica: they agree on the wrong
    /// answer, so no comparison sees it, and `Kv.apply` works it out itself.
    kv_if_absent_holds,
    /// A batch's conditions and versions are ignored, on every replica.
    kv_batch_conditions_ignored,
    // Bugs in the actions machine, each planted by rewriting the entry it
    // applies (`Actions.apply` checks the entry as it was): every replica
    // applies the same rewrite, so only the entry checks see them.
    /// A deliver doesn't wait for its run to be due.
    deliver_ignores_due,
    /// An expire ends leases before their deadline.
    expire_ignores_deadline,
    /// An expire ends the run's live lease whichever of its keys it names.
    expire_matches_delivery,
    /// A settle completes the run whatever delivery it names.
    settle_ignores_token,
    /// A failed delivery counts no failure.
    release_counts_nothing,
    /// A settle leaves its delivery's request record behind.
    settle_keeps_request,
    /// An extend holds from now, not from its deliver: past the ceiling.
    extend_ignores_held_since,
    /// Action entries apply at the replica's own clock.
    action_node_clock,
    /// A replica refuses a KV batch above its own transaction limit at apply,
    /// not only at its leader's gate: replicas with lower limits than the
    /// leader that proposed it then drop what the others applied.
    kv_refuses_over_own_limit,
    /// A leader proposes a batch above its own limit.
    leader_ignores_limit,
    /// The machine applies only the first half of a batch, on every replica,
    /// and calls it done: no comparison sees it.
    kv_stops_batch_short,
    /// The machine refuses a large batch on every replica, as a size check
    /// in its apply would: no comparison sees it.
    kv_refuses_large,
    /// Shard 0's leader proposes a create before it has applied every
    /// earlier term's entry, so it may choose an id an earlier leader's
    /// create already took.
    ns_ungated,
    /// The same, once the leader's commit index has passed every earlier
    /// term's entry, applied or not: what only a leader with an unapplied
    /// committed tail can show.
    ns_ready_on_commit,
    /// A leader that steps down keeps its in-flight namespace id: won
    /// again, it allocates past what the group created.
    ns_keeps_alloc,

    /// Whether the bug is in a node's transaction limit: a run with none
    /// never reaches it.
    pub fn needsTxnLimits(m: Mutant) bool {
        return m == .kv_refuses_over_own_limit or m == .leader_ignores_limit or m == .kv_refuses_large or m == .kv_stops_batch_short;
    }

    /// Whether the bug is in the actions machine: a run with no action
    /// entries never reaches it.
    pub fn inActions(m: Mutant) bool {
        return switch (m) {
            .none, .node_clock, .local_refusal, .map_order, .live_only_effect, .kv_replay_skips_batches, .kv_refused_commits, .kv_condition_local, .kv_if_absent_holds, .kv_batch_conditions_ignored, .kv_refuses_over_own_limit, .leader_ignores_limit, .kv_refuses_large, .kv_stops_batch_short, .ns_ungated, .ns_ready_on_commit, .ns_keeps_alloc => false,
            .deliver_ignores_due, .expire_ignores_deadline, .expire_matches_delivery, .settle_ignores_token, .release_counts_nothing, .settle_keeps_request, .extend_ignores_held_since, .action_node_clock => true,
        };
    }
};

/// What a family is told about the replica applying an entry. Only a
/// mutant reads it: a correct apply depends on the entry and the state.
pub const Replica = struct {
    mutant: Mutant,
    node_now: u64,
    /// The replica's wall clock, in ns, as a stamp would read it.
    node_wall_ns: u64 = 0,
    leader: bool,
    live: bool,
    /// The replica's own transaction limit; 0 for none.
    txn_limit: u16 = 0,
};

pub const Outcome = enum { ok, refused };

/// Counters over a small key space, written by every workload op: a
/// `kv_delete` removes its key, any other op adds to it. A write is
/// refused when its condition (fresh keys only, for a third of payloads)
/// fails; its staged write is then discarded.
pub const Counter = struct {
    pub const KEYS = 8;

    pub const Value = struct {
        count: u64,
        /// A fold over the payloads written, in log order.
        fold: u64,
        /// The stamp of the last write.
        stamp: u64,
    };

    map: std.AutoHashMapUnmanaged(u64, Value) = .empty,

    pub fn deinit(self: *Counter, allocator: Allocator) void {
        self.map.deinit(allocator);
    }

    /// Empty, as a restarted replica starts. `reserve` sizes the map for
    /// the entries replay will apply, as boot reserves its index map.
    pub fn reset(self: *Counter, allocator: Allocator, reserve: u32) void {
        self.map.clearRetainingCapacity();
        if (reserve > 0) self.map.ensureTotalCapacity(allocator, reserve) catch @panic("sim OOM");
    }

    pub fn owns(entry_type: u8) bool {
        return switch (@as(entry_mod.EntryType, @fromBackingInt(entry_type))) {
            .kv_put, .kv_delete, .stream_append, .queue_enqueue, .ts_write => true,
            else => false,
        };
    }

    pub fn apply(self: *Counter, allocator: Allocator, e: *const entry_mod.Entry, r: Replica) Outcome {
        const h = std.hash.Wyhash.hash(HASH_SEED, e.payload);
        const key = h % KEYS;
        if (e.header.entry_type == @backingInt(entry_mod.EntryType.kv_delete)) {
            return if (self.map.remove(key)) .ok else .refused;
        }
        const prev = self.map.get(key);
        var conditional = h % 3 == 0;
        if (r.mutant == .local_refusal) conditional = conditional and r.leader;
        if (conditional and prev != null) return .refused;
        // Staged: nothing is written until the entry is known to be ok.
        const base: Value = prev orelse .{ .count = 0, .fold = 0, .stamp = 0 };
        var staged: Value = .{
            .count = base.count + 1,
            .fold = std.hash.Wyhash.hash(base.fold, std.mem.asBytes(&h)),
            .stamp = if (r.mutant == .node_clock) r.node_now else e.header.timestamp_ns,
        };
        if (r.mutant == .live_only_effect and !r.live) staged.fold = base.fold;
        self.map.put(allocator, key, staged) catch @panic("sim OOM");
        return .ok;
    }

    pub fn checksum(self: *const Counter, mutant: Mutant) u64 {
        var hasher = std.hash.Wyhash.init(HASH_SEED);
        if (mutant == .map_order) {
            var it = self.map.iterator();
            while (it.next()) |kv| hashPair(&hasher, kv.key_ptr.*, kv.value_ptr.*);
            return hasher.final();
        }
        var keys: [KEYS]u64 = undefined;
        var n: usize = 0;
        var it = self.map.keyIterator();
        while (it.next()) |k| : (n += 1) keys[n] = k.*;
        std.mem.sortUnstable(u64, keys[0..n], {}, std.sort.asc(u64));
        for (keys[0..n]) |k| hashPair(&hasher, k, self.map.get(k).?);
        return hasher.final();
    }

    fn hashPair(hasher: *std.hash.Wyhash, key: u64, v: Value) void {
        hasher.update(std.mem.asBytes(&key));
        hasher.update(std.mem.asBytes(&v.count));
        hasher.update(std.mem.asBytes(&v.fold));
        hasher.update(std.mem.asBytes(&v.stamp));
    }
};

/// KV as the server applies it: its machine, on a store of this replica's
/// own. The store is strict, so an applier touching a key its entry didn't
/// declare fails the run; VOPR builds ReleaseSafe, where a store would only
/// count it.
pub const Kv = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    /// What an entry did, for a run's counts (`Counts`).
    pub const Effect = enum {
        other,
        /// Refused before staging anything.
        refused,
        /// A batch refused, which has staged writes by then.
        staged_refused,
        /// A put refused for its condition or cas_version.
        condition_refused,
        /// A batch refused for one of those, which has staged writes by then.
        batch_condition_refused,
        /// A put with a condition or cas_version that held.
        condition_held,
        /// A JSON patch merged into the value: a set below `$`.
        merged,
        /// A JSON patch that deleted a field.
        field_deleted,
    };

    /// Entries by outcome, each counted once however many replicas apply it.
    pub const Counts = struct {
        refusals: u64 = 0,
        staged_refusals: u64 = 0,
        merges: u64 = 0,
        field_deletes: u64 = 0,
        condition_refusals: u64 = 0,
        conditions_held: u64 = 0,
        batch_condition_refusals: u64 = 0,
        /// Counted by the simulator, which knows what a batch asked.
        batch_conditions_held: u64 = 0,

        pub fn count(self: *Counts, o: Effect) void {
            switch (o) {
                .other => {},
                .refused => self.refusals += 1,
                .staged_refused => {
                    self.refusals += 1;
                    self.staged_refusals += 1;
                },
                .condition_refused => {
                    self.refusals += 1;
                    self.condition_refusals += 1;
                },
                .batch_condition_refused => {
                    self.refusals += 1;
                    self.staged_refusals += 1;
                    self.condition_refusals += 1;
                    self.batch_condition_refusals += 1;
                },
                .condition_held => self.conditions_held += 1,
                .merged => self.merges += 1,
                .field_deleted => self.field_deletes += 1,
            }
        }
    };

    /// The families compared, in the order `checksums` returns them.
    pub const FAMILIES = [_]u16{ kv_machine.Values.ID, kv_machine.History.ID, kv_machine.NsCounts.ID };

    pub fn init(allocator: Allocator) Kv {
        var kv: Kv = .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
        kv.store.strict_keys = true;
        return kv;
    }

    pub fn deinit(self: *Kv) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    /// Empty, as a restarted replica starts; still strict.
    pub fn reset(self: *Kv) void {
        self.store.clear();
        std.debug.assert(self.store.strict_keys);
    }

    pub fn owns(entry_type: u8) bool {
        return kv_machine.owns(@fromBackingInt(entry_type));
    }

    /// Apply `e` as the shard does. A refusal is an answer, not an error,
    /// but it must leave every family as it was: `RefusalChangedState` if
    /// one moved. `ConditionDecidedWrong` if a put's condition came out other
    /// than the key as it stood says. Any other error (a halt, out of memory)
    /// is one a workload entry must never meet.
    pub fn apply(self: *Kv, e: *const entry_mod.Entry, r: Replica) (persistence.ApplyError || error{ RefusalChangedState, ConditionDecidedWrong })!Effect {
        // Every replica may agree on a wrong decision, so no checksum sees it.
        const holds: ?bool = if (r.mutant == .kv_condition_local) null else if (conditionalPut(e)) |p| self.conditionHolds(p, e.header.timestamp_ns) else null;
        if (r.mutant == .kv_replay_skips_batches and !r.live and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch)) return .other;
        if (r.mutant == .kv_refuses_over_own_limit and r.txn_limit != 0 and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch) and kv_entries.batchOpCount(e.payload) > r.txn_limit) return .staged_refused;
        if (r.mutant == .kv_refuses_large and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch) and kv_entries.batchOpCount(e.payload) > limits.MAX_OPS_PER_TXN / 2) return .staged_refused;
        var out: persistence.Result = .{};
        out.reset(e);
        const before = self.commits();
        if (r.mutant == .kv_refused_commits) {
            try self.applyCommittingRefusals(e, &out);
        } else if (r.mutant == .kv_stops_batch_short and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch)) {
            var buf: [8192]u8 = undefined;
            var short = e.*;
            short.payload = firstHalf(e.payload, &buf) orelse return error.Halt;
            try machine_mod.applyEntry(&kv_machine.machine, &self.store, &self.scratch, &short, &out);
        } else if (r.mutant == .kv_batch_conditions_ignored and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch)) {
            var buf: [8192]u8 = undefined;
            var plain = e.*;
            plain.payload = withoutConditions(e.payload, &buf) orelse return error.Halt;
            try machine_mod.applyEntry(&kv_machine.machine, &self.store, &self.scratch, &plain, &out);
        } else if (ignoresCondition(e, r)) {
            var buf: [8192]u8 = undefined;
            var body = conditionalPut(e).?;
            body.condition = .always;
            body.cas_version = null;
            const bytes = kv_entries.Put.encodeInto(&buf, body) catch return error.Halt;
            const plain = entry_mod.buildEntry(.kv_put, e.header.flags, e.header.term, e.header.index, e.header.timestamp_ns, bytes);
            try machine_mod.applyEntry(&kv_machine.machine, &self.store, &self.scratch, &plain, &out);
        } else {
            try machine_mod.applyEntry(&kv_machine.machine, &self.store, &self.scratch, e, &out);
        }
        if (holds) |h| if (h != (out.refused() == null)) return error.ConditionDecidedWrong;
        const refusal = out.refused() orelse
            return if (conditionalPut(e) != null) .condition_held else patchEffect(e);
        if (!std.mem.eql(u64, &before, &self.commits())) return error.RefusalChangedState;
        const is_batch = e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch);
        if (refusal.refusal.reason == .cas_mismatch or refusal.refusal.reason == .condition_failed) return if (is_batch) .batch_condition_refused else .condition_refused;
        return if (is_batch) .staged_refused else .refused;
    }

    /// Whether a mutant has this replica write `e` as a plain put.
    fn ignoresCondition(e: *const entry_mod.Entry, r: Replica) bool {
        const p = conditionalPut(e) orelse return false;
        return (r.mutant == .kv_condition_local and r.leader) or (r.mutant == .kv_if_absent_holds and p.condition == .if_absent);
    }

    /// The batch with no op asking anything, for `kv_batch_conditions_ignored`.
    fn withoutConditions(payload: []const u8, buf: []u8) ?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const batch = kv_entries.Batch.decodeAll(payload, &diag) catch return null;
        var ops: [limits.MAX_OPS_PER_TXN]kv_entries.BatchOp.Value = undefined;
        var n: usize = 0;
        var it = batch.ops.iterator();
        while (it.next() catch return null) |op| : (n += 1) {
            ops[n] = op;
            ops[n].condition = .always;
            ops[n].cas_version = null;
        }
        var w: schema.Writer = .{ .buf = buf };
        kv_entries.BatchOps.encodeSlice(&w, ops[0..n]) catch return null;
        return w.written();
    }

    /// The batch's first half of its ops, for `kv_stops_batch_short`.
    fn firstHalf(payload: []const u8, buf: []u8) ?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const batch = kv_entries.Batch.decodeAll(payload, &diag) catch return null;
        var ops: [limits.MAX_OPS_PER_TXN]kv_entries.BatchOp.Value = undefined;
        var n: usize = 0;
        var it = batch.ops.iterator();
        while (it.next() catch return null) |op| : (n += 1) ops[n] = op;
        var w: schema.Writer = .{ .buf = buf };
        kv_entries.BatchOps.encodeSlice(&w, ops[0 .. n / 2]) catch return null;
        return w.written();
    }

    /// The counter at `qkey` on this replica, 0 if there is none.
    pub fn counter(self: *Kv, qkey: []const u8) i64 {
        var kb: [kv_machine.MAX_KEY]u8 = undefined;
        const bytes = self.store.get(kv_machine.Values.ID, kv_machine.valueKey(&kb, kv_machine.Qualified.of(qkey))) orelse return 0;
        const current = kv_machine.Current.decode(bytes) catch return 0;
        if (current.tombstone or current.value.len != 8) return 0;
        return std.mem.readInt(i64, current.value[0..8], .little);
    }

    /// What `p` should be answered at `at_ns`, from the key as it stands.
    fn conditionHolds(self: *Kv, p: kv_entries.Put.Value, at_ns: u64) bool {
        var kb: [kv_machine.MAX_KEY]u8 = undefined;
        const bytes = self.store.get(kv_machine.Values.ID, kv_machine.valueKey(&kb, kv_machine.Qualified.of(p.key)));
        const current: ?kv_machine.Current = if (bytes) |b| (kv_machine.Current.decode(b) catch null) else null;
        const version: ?u64 = if (current) |c| (if (c.tombstone or c.expiredAt(at_ns)) null else c.version) else null;
        if (p.cas_version) |want| if (want != (version orelse 0)) return false;
        return switch (p.condition) {
            .always => true,
            .if_absent => version == null,
            .if_present => version != null,
        };
    }

    /// `e` as a put that asks something of its key.
    fn conditionalPut(e: *const entry_mod.Entry) ?kv_entries.Put.Value {
        if (e.header.entry_type != @backingInt(entry_mod.EntryType.kv_put)) return null;
        var diag: schema.Diagnostic = .{};
        const p = kv_entries.Put.decodeAll(e.payload, &diag) catch return null;
        return if (p.condition != .always or p.cas_version != null) p else null;
    }

    /// An applied JSON patch: merged into the value (a set below `$`), or a
    /// field deleted. A set at `$` writes the whole value and merges nothing.
    fn patchEffect(e: *const entry_mod.Entry) Effect {
        if (e.header.entry_type != @backingInt(entry_mod.EntryType.kv_json_patch)) return .other;
        var diag: schema.Diagnostic = .{};
        const p = kv_entries.JsonPatch.decodeAll(e.payload, &diag) catch return .other;
        if (std.mem.eql(u8, p.path, "$")) return .other;
        return if (p.document != null) .merged else .field_deleted;
    }

    /// `machine.applyEntry` without its guard against committing a refused
    /// entry: the bug `kv_refused_commits` plants.
    fn applyCommittingRefusals(self: *Kv, e: *const entry_mod.Entry, out: *persistence.Result) persistence.ApplyError!void {
        _ = self.scratch.reset(.retain_capacity);
        const arena = self.scratch.allocator();
        var keys = view_mod.KeySet.init(arena);
        try kv_machine.machine.keys(e, &keys);
        var view = view_mod.View.init(&self.store, arena, &keys);
        try kv_machine.machine.apply(&view, e, out);
        try view.commit();
    }

    /// Each family's count of records linked, replaced or unlinked.
    fn commits(self: *const Kv) [FAMILIES.len]u64 {
        var out: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &out) |fam, *c| c.* = self.store.families[fam].commits;
        return out;
    }

    pub fn checksums(self: *Kv) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
    }
};

/// How far a run's KV batches went past the limits of the nodes they reached.
/// Every count but the two refusals is of batches the machine applied: one it
/// refused left nothing behind, on any replica, so it reached nothing.
pub const TxnCounts = struct {
    /// Batches a leader refused, above its own limit: never proposed.
    leader_refused: u64 = 0,
    /// Batches the machine refused at apply, each counted once.
    machine_refused: u64 = 0,
    /// Batches applied, meaning the machine didn't refuse them, each counted
    /// once however many replicas apply it. `countTxn` checks each applied in
    /// full, by its last op.
    applied: u64 = 0,
    /// Of those, above the default limit, and above half the built-in most:
    /// "large" from here on.
    over_default: u64 = 0,
    over_half: u64 = 0,
    /// The most ops any batch applied.
    largest: u64 = 0,
    /// Applies of a large batch by a replica rebuilding.
    large_replayed: u64 = 0,
    /// Applies by a replica whose own limit is below the batch: live, and
    /// rebuilding.
    over_own_live: u64 = 0,
    over_own_replayed: u64 = 0,
    /// Of the live ones, by a leader, of an earlier term's batch: it came
    /// across a leader change, to a node that would have refused to propose it.
    over_own_across_leaders: u64 = 0,
    /// The same two, of large batches only.
    large_over_own_live: u64 = 0,
    large_across_leaders: u64 = 0,

    pub fn add(self: *TxnCounts, other: TxnCounts) void {
        inline for (comptime std.meta.fieldNames(TxnCounts)) |name| {
            if (comptime std.mem.eql(u8, name, "largest")) continue;
            @field(self, name) += @field(other, name);
        }
        self.largest = @max(self.largest, other.largest);
    }
};

/// The actions machine, as the server applies it, on a strict store of
/// this replica's own. Each entry is also checked against the state
/// before and after it, by the rules a lease must keep, independent of
/// the machine's own checks: those are what a mutant breaks.
pub const Actions = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    /// A mutant's rewritten payload.
    rewrite: []u8,

    pub const FAMILIES = [_]u16{ action_machine.Defs.ID, action_machine.Runs.ID, action_machine.Ready.ID, action_machine.Deadlines.ID, action_machine.Requests.ID };
    pub const NAMES = [_][]const u8{ "action.def", "action.run", "action.ready", "action.deadline", "action.request" };

    pub const Kind = enum { register, delete, invoke, deliver, settle, release, extend, expire };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        /// A settle, or an expire ending a lease, for a delivery made in an
        /// earlier term: one that outlived its leader.
        cross_term: bool = false,
        /// The rule the entry broke, if it broke one.
        broke: ?[]const u8 = null,
    };

    /// Entries by kind and outcome, each counted once.
    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),
        cross_term_settles: u64 = 0,
        cross_term_expires: u64 = 0,

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
            if (!o.cross_term) return;
            if (o.kind == .settle) self.cross_term_settles += 1 else self.cross_term_expires += 1;
        }

        pub fn add(self: *Counts, other: Counts) void {
            for (&self.applied, other.applied) |*a, b| a.* += b;
            for (&self.refused, other.refused) |*a, b| a.* += b;
            self.cross_term_settles += other.cross_term_settles;
            self.cross_term_expires += other.cross_term_expires;
        }
    };

    pub fn init(allocator: Allocator) Actions {
        var a: Actions = .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator), .rewrite = allocator.alloc(u8, ae.Expire.max_size) catch @panic("sim OOM") };
        a.store.strict_keys = true;
        return a;
    }

    pub fn deinit(self: *Actions) void {
        self.store.allocator.free(self.rewrite);
        self.store.deinit();
        self.scratch.deinit();
    }

    pub fn reset(self: *Actions) void {
        self.store.clear();
        std.debug.assert(self.store.strict_keys);
    }

    pub fn owns(entry_type: u8) bool {
        for (action_machine.machine.entry_types) |t| if (@backingInt(t) == entry_type) return true;
        return false;
    }

    pub fn read(self: *Actions) view_mod.ReadView {
        return .{ .store = &self.store };
    }

    /// A run's record, if it has one.
    pub fn run(self: *Actions, ns: []const u8, id: []const u8) ?action_machine.Run.Value {
        var b: [1 + 255 + ae.MAX_RUN_ID]u8 = undefined;
        const k = view_mod.key(&b, ns, id) catch return null;
        const got = self.read().get(action_machine.Runs, k) catch return null;
        return if (got) |r| r.v else null;
    }

    fn kindOf(t: u8) Kind {
        return switch (@as(entry_mod.EntryType, @fromBackingInt(t))) {
            .action_register => .register,
            .action_delete => .delete,
            .action_invoke => .invoke,
            .action_deliver => .deliver,
            .action_settle => .settle,
            .action_release => .release,
            .action_extend => .extend,
            else => .expire,
        };
    }

    /// The runs an entry names: at most one, or an expire's items.
    const MAX_TOUCHED = ae.MAX_EXPIRE_ITEMS;
    const Touched = struct {
        ns: []const u8,
        id: []const u8,
        before: ?action_machine.Run.Value,
    };

    /// Apply `e` as the shard does, then check it. A refusal must leave
    /// every family as it was (`RefusalChangedState`); any other error is
    /// one a workload entry must never meet.
    pub fn apply(self: *Actions, e: *const entry_mod.Entry, r: Replica) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        var diag: schema.Diagnostic = .{};
        const kind = kindOf(e.header.entry_type);
        const stamp = e.header.timestamp_ns;
        // The state before, of every run the entry names; decoded values
        // point into the store, so they are copied out first.
        var before_arena = std.heap.ArenaAllocator.init(self.store.allocator);
        defer before_arena.deinit();
        const ba = before_arena.allocator();
        var touched: std.ArrayListUnmanaged(Touched) = .empty;
        switch (kind) {
            .register, .delete => {},
            .expire => {
                const p = ae.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |item| try self.note(ba, &touched, item.ns, item.run);
            },
            inline else => |k| {
                const P = switch (k) {
                    .invoke => ae.Invoke,
                    .deliver => ae.Deliver,
                    .settle => ae.Settle,
                    .release => ae.Release,
                    .extend => ae.Extend,
                    else => unreachable,
                };
                const p = P.decodeAll(e.payload, &diag) catch return error.Halt;
                try self.note(ba, &touched, p.ns, p.run);
            },
        }

        // The action an invoke or deliver names, as it stood: whether it
        // should have been refused.
        const def_before: ?action_machine.Def.Value = switch (kind) {
            .invoke => self.def("default", (ae.Invoke.decodeAll(e.payload, &diag) catch return error.Halt).action),
            .deliver => self.def("default", (ae.Deliver.decodeAll(e.payload, &diag) catch return error.Halt).action),
            else => null,
        };

        const applied = try self.rewritten(e, r);
        var out: persistence.Result = .{};
        out.reset(&applied);
        const before = self.commits();
        try machine_mod.applyEntry(&action_machine.machine, &self.store, &self.scratch, &applied, &out);
        if (out.refused() != null) {
            if (!std.mem.eql(u64, &before, &self.commits())) return error.RefusalChangedState;
            return .{ .kind = kind, .refused = true, .broke = refusedWrongly(e, stamp, touched.items, def_before) };
        }
        var effect: Effect = .{ .kind = kind, .refused = false };
        effect.broke = self.check(e, stamp, touched.items, &effect.cross_term);
        return effect;
    }

    /// An action's definition, if it has one.
    fn def(self: *Actions, ns: []const u8, name: []const u8) ?action_machine.Def.Value {
        var b: [1 + 255 + ae.MAX_ACTION_NAME]u8 = undefined;
        const k = view_mod.key(&b, ns, name) catch return null;
        const got = self.read().get(action_machine.Defs, k) catch return null;
        return if (got) |d| d.v else null;
    }

    /// Why an entry the machine refused should have applied, if it should.
    fn refusedWrongly(e: *const entry_mod.Entry, stamp: u64, touched: []const Touched, def_before: ?action_machine.Def.Value) ?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const pre = if (touched.len == 1) touched[0].before else null;
        switch (kindOf(e.header.entry_type)) {
            .invoke => if (def_before != null and pre == null) return "an invoke of a registered action and a new run was refused",
            .deliver => {
                const p = ae.Deliver.decodeAll(e.payload, &diag) catch return null;
                const v = pre orelse return null;
                if (def_before != null and v.status == .ready and v.due_ns == p.due_ns and p.due_ns <= stamp and std.mem.eql(u8, v.action, p.action))
                    return "a deliver of a ready run, due, was refused";
            },
            .settle => {
                const p = ae.Settle.decodeAll(e.payload, &diag) catch return null;
                if (pre) |v| if (holdsDelivery(v, p.holder, p.deliveries, p.token) and std.mem.eql(u8, v.action, p.action)) return "a settle naming the live delivery was refused";
            },
            .release => {
                const p = ae.Release.decodeAll(e.payload, &diag) catch return null;
                if (pre) |v| if (holdsDelivery(v, p.holder, p.deliveries, p.token) and std.mem.eql(u8, v.action, p.action)) return "a release naming the live delivery was refused";
            },
            .extend => {
                const p = ae.Extend.decodeAll(e.payload, &diag) catch return null;
                if (pre) |v| if (holdsDelivery(v, p.holder, v.deliveries, p.token) and std.mem.eql(u8, v.action, p.action) and p.lease_ms == v.lease_ms and p.held_since_ns == v.delivered_ns)
                    return "an extend naming the live delivery and its own lease was refused";
            },
            else => {},
        }
        return null;
    }

    fn note(self: *Actions, ba: Allocator, touched: *std.ArrayListUnmanaged(Touched), ns: []const u8, id: []const u8) Allocator.Error!void {
        for (touched.items) |t| if (std.mem.eql(u8, t.ns, ns) and std.mem.eql(u8, t.id, id)) return;
        const got = self.run(ns, id);
        try touched.append(ba, .{ .ns = try ba.dupe(u8, ns), .id = try ba.dupe(u8, id), .before = if (got) |v| try copyRun(ba, v) else null });
    }

    fn copyRun(a: Allocator, v: action_machine.Run.Value) Allocator.Error!action_machine.Run.Value {
        var c = v;
        c.action = try a.dupe(u8, v.action);
        c.holder = try a.dupe(u8, v.holder);
        if (v.request_key) |k| c.request_key = try a.dupe(u8, k);
        return c;
    }

    fn after(stamp_ns: u64, millis: u64) u64 {
        return stamp_ns +| (millis *| std.time.ns_per_ms);
    }

    fn holdsDelivery(run_: action_machine.Run.Value, holder: []const u8, deliveries: u32, token: ae.Token.Value) bool {
        return run_.status == .leased and run_.deliveries == deliveries and run_.token.term == token.term and run_.token.index == token.index and std.mem.eql(u8, run_.holder, holder);
    }

    /// The rules an applied entry must keep, by the entry as it was
    /// proposed and the state either side of it.
    fn check(self: *Actions, e: *const entry_mod.Entry, stamp: u64, touched: []const Touched, cross_term: *bool) ?[]const u8 {
        var diag: schema.Diagnostic = .{};
        for (touched) |t| {
            const pre = t.before orelse continue;
            const post = self.run(t.ns, t.id) orelse return "an applied action entry deleted a run";
            const changed = post.status != pre.status or post.deliveries != pre.deliveries or post.failures != pre.failures or post.expiry_ns != pre.expiry_ns or post.ended != pre.ended;
            var failed: u32 = 0;
            switch (kindOf(e.header.entry_type)) {
                .deliver => {
                    const p = ae.Deliver.decodeAll(e.payload, &diag) catch unreachable;
                    if (pre.status != .ready or pre.due_ns != p.due_ns) return "a deliver applied to a run that wasn't ready at its due time";
                    if (pre.due_ns > stamp) return "a run was delivered before it was due";
                    if (pre.failures >= pre.max_attempts) return "a run that spent its attempts was delivered again";
                    // What it wrote is what it names, its lease the action's.
                    if (post.status != .leased or !std.mem.eql(u8, post.holder, p.holder) or post.token.term != e.header.term or post.token.index != e.header.index or post.deliveries != pre.deliveries + 1)
                        return "a deliver leased a run other than to the delivery it names";
                    if (!std.meta.eql(post.request_key != null, p.request_key != null) or (p.request_key != null and !std.mem.eql(u8, post.request_key.?, p.request_key.?)))
                        return "a deliver leased a run other than to the delivery it names";
                    if (post.lease_ms != p.lease_ms or post.delivered_ns != stamp or post.expiry_ns != after(stamp, p.lease_ms)) return "a deliver's lease doesn't run from its stamp for its lease";
                },
                .settle => {
                    const p = ae.Settle.decodeAll(e.payload, &diag) catch unreachable;
                    if (holdsDelivery(pre, p.holder, p.deliveries, p.token)) {
                        if (post.status != .completed or post.ended != .settled or !std.mem.eql(u8, post.result, p.result) or !std.mem.eql(u8, post.outcome, p.outcome))
                            return "a settle naming the live delivery didn't complete the run with its result";
                        cross_term.* = cross_term.* or pre.token.term < e.header.term;
                    } else if (changed) return "a settle that names no live delivery completed a run";
                },
                .release => {
                    const p = ae.Release.decodeAll(e.payload, &diag) catch unreachable;
                    if (holdsDelivery(pre, p.holder, p.deliveries, p.token)) {
                        if (!p.abandoned) failed = 1;
                        if (post.failures != pre.failures + failed) return "a run's failures don't count the deliveries that failed";
                        const spent = !p.abandoned and (!p.retry or pre.failures + 1 >= pre.max_attempts);
                        if (post.ended != (if (p.abandoned) action_machine.Ended.abandoned else .released)) return "a release naming the live delivery didn't end it as it said";
                        if (spent) {
                            if (post.status != .failed) return "a release that spent the run's attempts didn't fail it";
                        } else if (post.status != .ready or post.due_ns != after(stamp, p.delay_ms)) return "a released run isn't ready again after its delay";
                    } else if (changed) return "a release that names no live delivery ended one";
                },
                .extend => {
                    const p = ae.Extend.decodeAll(e.payload, &diag) catch unreachable;
                    if (holdsDelivery(pre, p.holder, pre.deliveries, p.token)) {
                        // The lease moves unless more than half of it is
                        // left, or the ceiling stops it.
                        const lease_ns = pre.lease_ms *| std.time.ns_per_ms;
                        const to = @min(after(stamp, p.lease_ms), after(pre.delivered_ns, ae.MAX_HOLD_MS));
                        const want = if (pre.expiry_ns -| stamp > lease_ns / 2 or to <= pre.expiry_ns) pre.expiry_ns else to;
                        if (post.expiry_ns != want) {
                            if (post.expiry_ns > after(pre.delivered_ns, ae.MAX_HOLD_MS)) return "a delivery is held past the hold ceiling";
                            return "an extend naming the live delivery didn't move its lease as it should";
                        }
                    } else if (changed) return "an extend that names no live delivery moved a lease";
                },
                .expire => if (pre.status == .leased and post.status != .leased) {
                    // The lease ended: an item named its live deadline, due.
                    const p = ae.Expire.decodeAll(e.payload, &diag) catch unreachable;
                    var named = false;
                    var it = p.items.iterator();
                    while (it.next() catch null) |item| {
                        if (std.mem.eql(u8, item.ns, t.ns) and std.mem.eql(u8, item.run, t.id) and item.expiry_ns == pre.expiry_ns) named = true;
                    }
                    if (!named) return "an expire ended a lease none of its items named the deadline of";
                    if (pre.expiry_ns > stamp) return "an expire ended a lease before its deadline";
                    failed = 1;
                    cross_term.* = cross_term.* or pre.token.term < e.header.term;
                } else if (changed) return "an expire changed a run without ending its lease",
                else => {},
            }
            if (post.failures != pre.failures + failed) return "a run's failures don't count the deliveries that failed";
        }
        for (touched) |t| if (self.run(t.ns, t.id)) |post| if (self.ruleOfRun(t.ns, t.id, post)) |why| return why;
        return null;
    }

    /// What every run must be, whatever entry left it so.
    fn ruleOfRun(self: *Actions, ns: []const u8, id: []const u8, v: action_machine.Run.Value) ?[]const u8 {
        if (v.failures >= v.max_attempts and v.status != .failed and v.status != .timed_out) return "a run that spent its attempts didn't end";
        var b: [512]u8 = undefined;
        var kb: [512]u8 = undefined;
        switch (v.status) {
            .ready => {
                const k = view_mod.key(&kb, ns, action_machine.readyKey(&b, v.action, v.due_ns, id)) catch return null;
                if (self.store.get(action_machine.Ready.ID, k) == null) return "a ready run has no ready key at its due time";
            },
            .leased => {
                if (v.expiry_ns > v.delivered_ns +| ae.MAX_HOLD_MS * std.time.ns_per_ms) return "a delivery is held past the hold ceiling";
                const k = view_mod.key(&kb, "", action_machine.deadlineKey(&b, v.expiry_ns, ns, id)) catch return null;
                if (self.store.get(action_machine.Deadlines.ID, k) == null) return "a leased run has no deadline key at its expiry";
            },
            else => {},
        }
        return null;
    }

    /// Every request record names its run's live delivery. Walks the
    /// family, so the simulator calls it when it samples.
    pub fn danglingRequest(self: *Actions) ?[]const u8 {
        var it = self.store.range(action_machine.Requests.ID, "");
        while (it.next()) |rec| {
            const k = rec.key();
            const ns = k[1..][0..k[0]];
            const req = (action_machine.Requests.Value.decode(rec.value) catch return "a request record doesn't decode").v;
            const v = self.run(ns, req.run) orelse return "a request record names no run";
            if (v.status != .leased or v.token.term != req.term or v.token.index != req.index) return "a request record names a delivery that isn't live";
        }
        return null;
    }

    /// The entry a mutant applies in place of `e`; `e` itself otherwise.
    fn rewritten(self: *Actions, e: *const entry_mod.Entry, r: Replica) persistence.ApplyError!entry_mod.Entry {
        var out = e.*;
        var diag: schema.Diagnostic = .{};
        const stamp = e.header.timestamp_ns;
        switch (r.mutant) {
            .action_node_clock => out.header.timestamp_ns = r.node_wall_ns,
            .deliver_ignores_due => if (kindOf(e.header.entry_type) == .deliver) {
                const p = ae.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
                out.header.timestamp_ns = @max(stamp, p.due_ns);
            },
            .expire_ignores_deadline => if (kindOf(e.header.entry_type) == .expire) {
                out.header.timestamp_ns = std.math.maxInt(u64) / 2;
            },
            .expire_matches_delivery => if (kindOf(e.header.entry_type) == .expire) {
                // An item for a key past due ends its run's live lease.
                const p = ae.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var w: schema.Writer = .{ .buf = self.rewrite[4..] };
                var it = p.items.iterator();
                var latest = stamp;
                while (it.next() catch return error.Halt) |item| {
                    var moved = item;
                    if (item.expiry_ns <= stamp) if (self.run(item.ns, item.run)) |v| if (v.status == .leased) {
                        moved.expiry_ns = v.expiry_ns;
                        latest = @max(latest, v.expiry_ns);
                    };
                    ae.ExpireItem.encode(&w, moved) catch return error.Halt;
                }
                out.payload = ae.Expire.encodeInto(self.rewrite, .{ .items = .{ .bytes = try self.scratch.allocator().dupe(u8, w.buf[0..w.pos]), .count = p.items.count } }) catch return error.Halt;
                out.header.timestamp_ns = latest;
            },
            .settle_ignores_token => if (kindOf(e.header.entry_type) == .settle) {
                var p = ae.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
                if (self.run(p.ns, p.run)) |v| if (v.status == .leased) {
                    p.holder = try self.scratch.allocator().dupe(u8, v.holder);
                    p.deliveries = v.deliveries;
                    p.token = v.token;
                    p.expiry_ns = v.expiry_ns;
                    p.request_key = if (v.request_key) |k| try self.scratch.allocator().dupe(u8, k) else null;
                };
                out.payload = ae.Settle.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .settle_keeps_request => if (kindOf(e.header.entry_type) == .settle) {
                var p = ae.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
                p.request_key = null;
                out.payload = ae.Settle.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .release_counts_nothing => if (kindOf(e.header.entry_type) == .release) {
                var p = ae.Release.decodeAll(e.payload, &diag) catch return error.Halt;
                p.abandoned = true;
                out.payload = ae.Release.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .extend_ignores_held_since => if (kindOf(e.header.entry_type) == .extend) {
                // Held from now: the run's start moves, so the extend is
                // still its lease's, and the ceiling with it.
                const p = ae.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
                if (self.run(p.ns, p.run)) |v| if (v.status == .leased and v.delivered_ns == p.held_since_ns) {
                    var moved = p;
                    moved.held_since_ns = stamp;
                    try self.moveStart(p.ns, p.run, stamp);
                    out.payload = ae.Extend.encodeInto(self.rewrite, moved) catch return error.Halt;
                };
            },
            else => {},
        }
        return out;
    }

    /// Set a run's `delivered_ns` in place: `extend_ignores_held_since`'s
    /// state half. Its value is copied, so the store owns what it holds.
    fn moveStart(self: *Actions, ns: []const u8, id: []const u8, to: u64) persistence.ApplyError!void {
        var b: [1 + 255 + ae.MAX_RUN_ID]u8 = undefined;
        const k = view_mod.key(&b, ns, id) catch return error.Halt;
        const node = self.store.find(action_machine.Runs.ID, k) orelse return;
        var v = (action_machine.Runs.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
        v.delivered_ns = to;
        const bytes = try self.store.allocator.alloc(u8, action_machine.Run.encodedSize(v) catch return error.Halt);
        _ = action_machine.Run.encodeInto(bytes, v) catch return error.Halt;
        self.store.replace(action_machine.Runs.ID, node, bytes);
    }

    fn commits(self: *const Actions) [FAMILIES.len]u64 {
        var out: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &out) |fam, *c| c.* = self.store.families[fam].commits;
        return out;
    }

    pub fn checksums(self: *Actions) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
    }
};

/// Shard 0's namespace registry: the server's handler, on a table of its
/// own. The simulator proposes creates through it as shard 0's leader does
/// (`proposeCreateOn`) and applies them through its replay path. Nothing
/// is published: shard 0's create path reads its registry, never the table.
pub const Namespaces = struct {
    handler: ns_handler.NamespaceHandler,

    /// `past_next`: created, at an id beyond the next free one. Every
    /// replica agrees on the gap, so no checksum shows it.
    pub const Effect = enum { created, past_next, stale_id, refused };

    /// Creates by outcome, each counted once however many replicas apply it.
    pub const Counts = struct {
        created: u64 = 0,
        past_next: u64 = 0,
        stale_ids: u64 = 0,
        refusals: u64 = 0,

        pub fn count(self: *Counts, e: Effect) void {
            switch (e) {
                .created => self.created += 1,
                .past_next => {
                    self.created += 1;
                    self.past_next += 1;
                },
                .stale_id => self.stale_ids += 1,
                .refused => self.refusals += 1,
            }
        }
    };

    pub fn init(allocator: Allocator) Namespaces {
        return .{ .handler = ns_handler.NamespaceHandler.init(allocator) catch @panic("sim OOM") };
    }

    pub fn deinit(self: *Namespaces) void {
        self.handler.deinit();
    }

    /// Empty, as a restarted replica starts.
    pub fn reset(self: *Namespaces) void {
        const allocator = self.handler.allocator;
        self.handler.deinit();
        self.* = init(allocator);
    }

    pub fn owns(entry_type: u8) bool {
        return entry_type == @backingInt(entry_mod.EntryType.namespace_create);
    }

    /// Apply `e` as shard 0 does. An error is one a workload create must
    /// never meet.
    pub fn apply(self: *Namespaces, e: *const entry_mod.Entry) persistence.ApplyError!Effect {
        var out: persistence.Result = .{};
        out.reset(e);
        const next = self.handler.next_id;
        try self.handler.replayEntry(e, &out);
        const r = out.refused() orelse {
            var diag: schema.Diagnostic = .{};
            const c = namespace_entries.Create.decodeAll(e.payload, &diag) catch return error.Halt;
            return if (c.id == next) .created else .past_next;
        };
        return if (std.mem.eql(u8, r.refusal.message, ns_handler.NamespaceHandler.STALE_ID_MESSAGE)) .stale_id else .refused;
    }

    /// The registry, the creates this log refused, and the next id, in no
    /// order of the maps': each name and each refused hash is hashed alone
    /// and the hashes summed.
    pub fn checksum(self: *const Namespaces) u64 {
        const h = &self.handler;
        var sum: u64 = std.hash.Wyhash.hash(HASH_SEED, std.mem.asBytes(&h.next_id));
        var it = h.namespaces.iterator();
        while (it.next()) |kv| {
            var hasher = std.hash.Wyhash.init(HASH_SEED);
            hasher.update(kv.key_ptr.*);
            const m = kv.value_ptr.*;
            hasher.update(std.mem.asBytes(&[3]u64{ m.id, m.created_at_ns, m.created_index }));
            sum +%= hasher.final();
        }
        var refused = h.refused_hashes.keyIterator();
        while (refused.next()) |hash| sum +%= std.hash.Wyhash.hash(HASH_SEED +% 1, std.mem.asBytes(hash));
        return sum;
    }
};

const testing = std.testing;

fn put(c: *Counter, payload: []const u8, stamp: u64, r: Replica) Outcome {
    var e = entry_mod.buildEntry(.kv_put, 0, 1, 1, stamp, payload);
    e.header.crc32c = e.computeCrc();
    return c.apply(testing.allocator, &e, r);
}

test "families: the checksum is the same whatever the map's capacity" {
    const live: Replica = .{ .mutant = .none, .node_now = 0, .leader = false, .live = true };
    var a: Counter = .{};
    defer a.deinit(testing.allocator);
    var b: Counter = .{};
    defer b.deinit(testing.allocator);
    b.reset(testing.allocator, 4096);
    var payload: [8]u8 = undefined;
    for (0..64) |i| {
        std.mem.writeInt(u64, &payload, i, .little);
        try testing.expectEqual(put(&a, &payload, i + 1, live), put(&b, &payload, i + 1, live));
    }
    try testing.expectEqual(a.checksum(.none), b.checksum(.none));
    try testing.expect(a.checksum(.map_order) != b.checksum(.map_order));
}

test "families: a refused put leaves the state as it was" {
    const live: Replica = .{ .mutant = .none, .node_now = 0, .leader = false, .live = true };
    var c: Counter = .{};
    defer c.deinit(testing.allocator);
    var payload: [8]u8 = undefined;
    // A conditional payload, put twice: the second finds its key taken.
    var i: u64 = 0;
    while (std.hash.Wyhash.hash(HASH_SEED, std.mem.asBytes(&i)) % 3 != 0) i += 1;
    std.mem.writeInt(u64, &payload, i, .little);
    try testing.expectEqual(Outcome.ok, put(&c, &payload, 1, live));
    const before = c.checksum(.none);
    try testing.expectEqual(Outcome.refused, put(&c, &payload, 2, live));
    try testing.expectEqual(before, c.checksum(.none));
}

test "families: a KV replica's store is strict, and stays so once reset" {
    var kv = Kv.init(testing.allocator);
    defer kv.deinit();
    try testing.expect(kv.store.strict_keys);
    var body_buf: [64]u8 = undefined;
    const p = try @import("../protocol/kv_entries.zig").Put.encodeInto(&body_buf, .{ .key = "k", .value = "v", .ttl_ms = null, .condition = .always, .cas_version = null });
    var e = entry_mod.buildEntry(.kv_put, 0, 1, 1, 1, p);
    e.header.crc32c = e.computeCrc();
    const empty = kv.checksums();
    try testing.expectEqual(Kv.Effect.other, try kv.apply(&e, .{ .mutant = .none, .node_now = 0, .leader = false, .live = true }));
    try testing.expect(kv.checksums()[0] != empty[0]);
    kv.reset();
    try testing.expect(kv.store.strict_keys);
    try testing.expectEqual(empty, kv.checksums());
}

/// A run's life on one replica, for the tests: entries applied in order,
/// one index after another, each a second after the one before.
const ActionsTest = struct {
    a: Actions,
    index: u64 = 0,
    stamp: u64 = 1_000 * std.time.ns_per_s,
    buf: [4096]u8 = undefined,
    mutant: Mutant = .none,

    fn init() ActionsTest {
        return .{ .a = Actions.init(testing.allocator) };
    }

    fn deinit(t: *ActionsTest) void {
        t.a.deinit();
    }

    fn at(t: *ActionsTest, comptime P: type, comptime et: entry_mod.EntryType, v: P.Value, stamp: u64) !Actions.Effect {
        t.index += 1;
        t.stamp = stamp;
        const payload = try P.encodeInto(&t.buf, v);
        var e = entry_mod.buildEntry(et, 0, 1, t.index, stamp, payload);
        e.header.crc32c = e.computeCrc();
        return t.a.apply(&e, .{ .mutant = t.mutant, .node_now = 0, .node_wall_ns = stamp + 7 * std.time.ns_per_s, .leader = false, .live = true });
    }

    fn next(t: *ActionsTest, comptime P: type, comptime et: entry_mod.EntryType, v: P.Value) !Actions.Effect {
        return t.at(P, et, v, t.stamp + std.time.ns_per_s);
    }

    /// Registered, invoked and delivered to `w`; the deliver's token.
    fn delivered(t: *ActionsTest, lease_ms: u64, max_attempts: u32, request_key: ?[]const u8) !ae.Token.Value {
        _ = try t.next(ae.Register, .action_register, .{ .ns = "default", .name = "act", .owner = "", .lease_ms = lease_ms, .max_attempts = max_attempts });
        _ = try t.next(ae.Invoke, .action_invoke, .{ .ns = "default", .action = "act", .run = "r", .input = "", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null, .caller_shard = null });
        const due = t.stamp;
        const d = try t.next(ae.Deliver, .action_deliver, .{ .ns = "default", .action = "act", .run = "r", .due_ns = due, .holder = "w", .request_key = request_key, .lease_ms = lease_ms });
        try testing.expect(!d.refused);
        return .{ .term = 1, .index = t.index };
    }

    fn live(t: *ActionsTest) action_machine.Run.Value {
        return t.a.run("default", "r").?;
    }

    fn expire(t: *ActionsTest, expiry_ns: u64, stamp: u64) !?[]const u8 {
        return expireAt(t, expiry_ns, stamp);
    }
};

test "families: an action run's entries keep their rules, and each refused one changes nothing" {
    var t = ActionsTest.init();
    defer t.deinit();
    const token = try t.delivered(5_000, 2, "k");
    try testing.expectEqual(@as(?[]const u8, null), t.a.danglingRequest());
    // A stale token is refused; the live one fails the run, retried.
    const stale = try t.next(ae.Release, .action_release, .{ .ns = "default", .action = "act", .run = "r", .holder = "w", .request_key = "k", .deliveries = 1, .token = .{ .term = 1, .index = 99 }, .expiry_ns = t.live().expiry_ns, .abandoned = false, .retry = true, .delay_ms = 0, .@"error" = "" });
    try testing.expect(stale.refused);
    const rel = try t.next(ae.Release, .action_release, .{ .ns = "default", .action = "act", .run = "r", .holder = "w", .request_key = "k", .deliveries = 1, .token = token, .expiry_ns = t.live().expiry_ns, .abandoned = false, .retry = true, .delay_ms = 0, .@"error" = "" });
    try testing.expectEqual(@as(?[]const u8, null), rel.broke);
    try testing.expectEqual(@as(u32, 1), t.live().failures);
    try testing.expectEqual(@as(?[]const u8, null), t.a.danglingRequest());
}

/// What a mutant breaks in a run's life: the rule `steps` reports.
fn brokenUnder(mutant: Mutant, comptime steps: fn (*ActionsTest) anyerror!?[]const u8) !?[]const u8 {
    var t = ActionsTest.init();
    defer t.deinit();
    t.mutant = mutant;
    return steps(&t);
}

const mutant_steps = struct {
    fn earlyDeliver(t: *ActionsTest) !?[]const u8 {
        _ = try t.next(ae.Register, .action_register, .{ .ns = "default", .name = "act", .owner = "", .lease_ms = 5_000, .max_attempts = 2 });
        _ = try t.next(ae.Invoke, .action_invoke, .{ .ns = "default", .action = "act", .run = "r", .input = "", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null, .caller_shard = null });
        const due = t.stamp;
        // Due at `due`, delivered a second before it.
        return (try t.at(ae.Deliver, .action_deliver, .{ .ns = "default", .action = "act", .run = "r", .due_ns = due, .holder = "w", .request_key = null, .lease_ms = 5_000 }, due - std.time.ns_per_s)).broke;
    }

    fn earlyExpire(t: *ActionsTest) !?[]const u8 {
        _ = try t.delivered(5_000, 2, null);
        return t.expire(t.live().expiry_ns, t.stamp + std.time.ns_per_s);
    }

    fn staleKeyExpire(t: *ActionsTest) !?[]const u8 {
        const token = try t.delivered(10_000, 2, null);
        const old = t.live().expiry_ns;
        // Extended past half its lease: the old key stays, now past due.
        _ = try t.at(ae.Extend, .action_extend, .{ .ns = "default", .action = "act", .run = "r", .holder = "w", .token = token, .expiry_ns = old, .held_since_ns = t.live().delivered_ns, .lease_ms = 10_000 }, t.stamp + 6 * std.time.ns_per_s);
        return t.expire(old, old + std.time.ns_per_s);
    }

    fn staleSettle(t: *ActionsTest) !?[]const u8 {
        _ = try t.delivered(5_000, 2, null);
        return (try t.next(ae.Settle, .action_settle, .{ .ns = "default", .action = "act", .run = "r", .holder = "other", .request_key = null, .deliveries = 1, .token = .{ .term = 1, .index = 99 }, .expiry_ns = t.live().expiry_ns, .outcome = "", .result = "" })).broke;
    }

    fn failedRelease(t: *ActionsTest) !?[]const u8 {
        const token = try t.delivered(5_000, 3, null);
        return (try t.next(ae.Release, .action_release, .{ .ns = "default", .action = "act", .run = "r", .holder = "w", .request_key = null, .deliveries = 1, .token = token, .expiry_ns = t.live().expiry_ns, .abandoned = false, .retry = true, .delay_ms = 0, .@"error" = "" })).broke;
    }

    fn settleWithKey(t: *ActionsTest) !?[]const u8 {
        const token = try t.delivered(5_000, 2, "k");
        const s = try t.next(ae.Settle, .action_settle, .{ .ns = "default", .action = "act", .run = "r", .holder = "w", .request_key = "k", .deliveries = 1, .token = token, .expiry_ns = t.live().expiry_ns, .outcome = "", .result = "" });
        return s.broke orelse t.a.danglingRequest();
    }

    fn longHold(t: *ActionsTest) !?[]const u8 {
        const lease = ae.MAX_LEASE_MS;
        const token = try t.delivered(lease, 2, null);
        const since = t.live().delivered_ns;
        // Past half the lease: the extend moves it, to the ceiling at most.
        return (try t.at(ae.Extend, .action_extend, .{ .ns = "default", .action = "act", .run = "r", .holder = "w", .token = token, .expiry_ns = t.live().expiry_ns, .held_since_ns = since, .lease_ms = lease }, since + 7 * 60 * 60 * std.time.ns_per_s)).broke;
    }
};

fn expireAt(t: *ActionsTest, expiry_ns: u64, stamp: u64) !?[]const u8 {
    var ib: [512]u8 = undefined;
    const item = try ae.ExpireItem.encodeInto(&ib, .{ .ns = "default", .action = "act", .run = "r", .expiry_ns = expiry_ns, .holder = "w", .request_key = null });
    return (try t.at(ae.Expire, .action_expire, .{ .items = .{ .bytes = item, .count = 1 } }, stamp)).broke;
}

test "families: each actions mutant breaks the rule it plants a bug against, and none breaks it unplanted" {
    const Case = struct { mutant: Mutant, steps: fn (*ActionsTest) anyerror!?[]const u8, want: []const u8 };
    const cases = [_]Case{
        .{ .mutant = .deliver_ignores_due, .steps = mutant_steps.earlyDeliver, .want = "a run was delivered before it was due" },
        .{ .mutant = .expire_ignores_deadline, .steps = mutant_steps.earlyExpire, .want = "an expire ended a lease before its deadline" },
        .{ .mutant = .expire_matches_delivery, .steps = mutant_steps.staleKeyExpire, .want = "an expire ended a lease none of its items named the deadline of" },
        .{ .mutant = .settle_ignores_token, .steps = mutant_steps.staleSettle, .want = "a settle that names no live delivery completed a run" },
        .{ .mutant = .release_counts_nothing, .steps = mutant_steps.failedRelease, .want = "a run's failures don't count the deliveries that failed" },
        .{ .mutant = .settle_keeps_request, .steps = mutant_steps.settleWithKey, .want = "a request record names a delivery that isn't live" },
        .{ .mutant = .extend_ignores_held_since, .steps = mutant_steps.longHold, .want = "a delivery is held past the hold ceiling" },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("mutant {t}\n", .{c.mutant});
        try testing.expectEqual(@as(?[]const u8, null), try brokenUnder(.none, c.steps));
        try testing.expectEqualStrings(c.want, (try brokenUnder(c.mutant, c.steps)) orelse "nothing broken");
    }
}

fn nsCreate(n: *Namespaces, index: u64, name: []const u8, id: u32) !Namespaces.Effect {
    var buf: [256]u8 = undefined;
    const p = try namespace_entries.Create.encodeInto(&buf, .{ .name = name, .id = id });
    var e = entry_mod.buildEntry(.namespace_create, 0, 1, index, index, p);
    e.header.crc32c = e.computeCrc();
    return n.apply(&e);
}

test "families: a namespace create refused for its id folds into the checksum, and resets" {
    const first = @import("../namespace/table.zig").FIRST_ALLOCATED;
    var a = Namespaces.init(testing.allocator);
    defer a.deinit();
    var b = Namespaces.init(testing.allocator);
    defer b.deinit();
    const empty = a.checksum();
    try testing.expectEqual(Namespaces.Effect.created, try nsCreate(&a, 1, "orders", first));
    try testing.expectEqual(Namespaces.Effect.created, try nsCreate(&b, 1, "orders", first));
    try testing.expectEqual(a.checksum(), b.checksum());
    const created = a.checksum();
    // "ledger" asks for the id "orders" took: refused on both alike, and
    // the refusal is part of the state.
    try testing.expectEqual(Namespaces.Effect.stale_id, try nsCreate(&a, 2, "ledger", first));
    try testing.expect(a.checksum() != created);
    try testing.expect(a.checksum() != b.checksum());
    try testing.expectEqual(Namespaces.Effect.stale_id, try nsCreate(&b, 2, "ledger", first));
    try testing.expectEqual(a.checksum(), b.checksum());
    a.reset();
    try testing.expectEqual(empty, a.checksum());
}

test "families: a namespace create past the next free id is told apart from one at it" {
    const first = @import("../namespace/table.zig").FIRST_ALLOCATED;
    var n = Namespaces.init(testing.allocator);
    defer n.deinit();
    try testing.expectEqual(Namespaces.Effect.created, try nsCreate(&n, 1, "orders", first));
    try testing.expectEqual(Namespaces.Effect.past_next, try nsCreate(&n, 2, "ledger", first + 2));
    try testing.expectEqual(Namespaces.Effect.created, try nsCreate(&n, 3, "audit", first + 3));
}
