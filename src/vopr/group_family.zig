//! Consumer groups, as `queue_family.Queues` is for queues: every replica
//! applies the same entries through the server's own group machine
//! (`stream/group_machine.zig`) and stream projection, on the queue
//! family's store, as a shard keeps queues and groups in one store and one
//! held-bytes record. The machine reads its streams through the real
//! `Streams` reader; stream appends, trims and deletes apply to the real
//! projection, and trims and deletes reach the machine through its
//! observer, as the stream applier hands them on. Appends and deletes
//! reach the streams machine (`stream/machine.zig`) the same way, in the
//! shard's order, so a stream's metadata is checked beside its groups.
//!
//! The oracle is a ledger of its own: each group's records (state, holder,
//! token, counts, error length), its settings, cursor and counters, and
//! each stream's floor, generation and group count, kept from the entries
//! by rules and arithmetic written here: lease and nack times, the extend
//! clamp, held bytes and answer bytes are the ledger's own, not the
//! machine's or the delivery model's. The machine's key builders are used
//! only to find records in the store, never to decide what an entry does. Each entry's
//! answer is checked against what the ledger says it must be, and after it
//! applies every group's store state is checked against the ledger, so a
//! machine that picks, ends, counts or cleans otherwise is caught at the
//! entry that did it. What must hold of any store (each record's keys, the
//! counts, the held bytes, the shard's held bytes across queues and
//! groups) is read from the store itself.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const view_mod = @import("../contract/view.zig");
const schema = @import("../protocol/schema.zig");
const ge = @import("../protocol/group_entries.zig");
const ae = @import("../protocol/action_entries.zig");
const gm = @import("../stream/group_machine.zig");
const sm = @import("../stream/machine.zig");
const qm = @import("../queue/machine.zig");
const stream_proj = @import("../projection/stream.zig");
const streams_mod = @import("../contract/streams.zig");
const families = @import("families.zig");
const qe = @import("../protocol/queue_entries.zig");

const Allocator = std.mem.Allocator;
const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const StreamID = gm.StreamID;
const StreamKey = gm.StreamKey;
const NamespaceId = gm.NamespaceId;
const Store = store_mod.Store;
const Replica = families.Replica;
const Status = gm.Status;
const Ended = gm.Ended;
const Why = gm.Why;
/// Room for any key the group machine's key builders write: a scope, its
/// separator and a due time, or an expiry and a namespace, then a record id.
const KeyBuf = [gm.SCOPE_LEN + 1 + 8 + gm.ID_LEN]u8;

pub const NS: NamespaceId = .default;
pub const STREAMS = [_][]const u8{ "s0", "s1" };
pub const GROUPS = [_][]const u8{ "g0", "g1", "g2" };

/// The group machine's families, as the checksum walks them.
pub const FAMILIES = [_]u16{ gm.Metas.ID, gm.Items.ID, gm.Dead.ID, gm.Ready.ID, gm.Deadlines.ID, gm.Streams_.ID, sm.Metas.ID, sm.Retained.ID, sm.Records.ID, sm.Reclaim.ID };
pub const NAMES = [_][]const u8{ "group.meta", "group.item", "group.dead", "group.ready", "group.deadline", "group.stream", "stream.meta", "stream.retention", "stream.record", "stream.reclaim", "stream.index" };
/// The families' checksums, then the stream projection's.
pub const SUMS = FAMILIES.len + 1;

/// A stream entry in the simulator's log: an append of `[records:u32]`
/// under the stream's name; a trim through the id the key carries
/// (`[ms:u64 BE][seq:u64 BE]`), its value the stream's name hash as the
/// observer reads it; a delete under the stream's name. The projection and
/// the observer see each as a shard's stream applier hands them on.
pub fn streamEntry(buf: []u8, key: []const u8, value: []const u8) ?[]const u8 {
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = NS, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
    const n = cmd.serialize(buf) orelse return null;
    return buf[0..n];
}

pub fn trimKey(up_to: StreamID) [16]u8 {
    var k: [16]u8 = undefined;
    std.mem.writeInt(u64, k[0..8], up_to.timestamp_ms, .big);
    std.mem.writeInt(u64, k[8..16], up_to.sequence, .big);
    return k;
}

pub fn streamHash(name: []const u8) [8]u8 {
    return gm.streamOf(NS, name)[4..12].*;
}

/// Bytes one append's records take, as the streams machine counts them.
const RECORD_BYTES: u32 = 24;

/// The most an append value of this family takes: three records.
pub const MAX_APPEND_VALUE = 8 + 3 * RECORD_BYTES;

/// An append value of `n` records, as a client's is: partition 0, then a
/// batch of `n` records, `RECORD_BYTES` each with their framing.
pub fn appendValue(buf: *[MAX_APPEND_VALUE]u8, n: u32) []const u8 {
    return stream_proj.appendValueOf(buf, n, RECORD_BYTES - 6);
}

/// The records an append value of this family holds; null for another's.
pub fn appendCount(value: []const u8) ?u32 {
    if (value.len < 8 or std.mem.readInt(u32, value[0..4], .little) != 0) return null;
    const n = std.mem.readInt(u32, value[4..8], .little);
    if (n < 1 or n > 3 or value.len != 8 + n * RECORD_BYTES) return null;
    return n;
}

// The ledger's arithmetic, written here rather than taken from the
// delivery model or the machine, so a bug in theirs can't pass as the
// ledger's own. Each restates a rule the wire or the group docs set.

/// `ms` after `stamp`, saturating: a lease's expiry, a nack's due time.
fn afterMs(stamp_ns: u64, ms: u64) u64 {
    return stamp_ns +| (ms *| std.time.ns_per_ms);
}

/// Where an extend moves a lease: a full lease from now, but never past the
/// hold ceiling from its deliver.
fn extendTo(stamp_ns: u64, lease_ms: u64, delivered_ns: u64) u64 {
    const lease = afterMs(stamp_ns, lease_ms);
    const ceiling = afterMs(delivered_ns, ae.MAX_HOLD_MS);
    return if (lease < ceiling) lease else ceiling;
}

/// What a record a group holds counts against the caps: its error text and
/// the per-message overhead (`qe.PER_MESSAGE`, the queues' wire figure); the
/// stream holds its payload.
fn heldOf(err_len: usize) u64 {
    return err_len + qe.PER_MESSAGE;
}

/// The most an append's records take in a read's answer: each record's
/// framing (28 bytes: id, headers and length fields) and its bytes twice
/// over, as the stream read's answer budget counts them.
fn answerBytes(byte_len: u32, record_count: u32) u64 {
    return 2 * @as(u64, byte_len) + 28 * @as(u64, record_count);
}

// ═══════════════════════════════════════════════════════════════════════════
// The ledger
// ═══════════════════════════════════════════════════════════════════════════

const LItem = struct {
    status: Status,
    due_ns: u64 = 0,
    deliveries: u32 = 0,
    failures: u32 = 0,
    holder: []const u8 = "",
    token: ae.Token.Value = .{ .term = 0, .index = 0 },
    lease_ms: u64 = 0,
    delivered_ns: u64 = 0,
    expiry_ns: u64 = 0,
    ended: Ended = .none,
    started_ns: u64 = 0,
    /// Its holder's last sign of life: deliver, steal or any applied touch.
    touched_ns: u64 = 0,
    err_len: usize = 0,
    dead_ns: u64 = 0,
};

fn recKey(r: StreamID) u128 {
    return (@as(u128, r.timestamp_ms) << 64) | r.sequence;
}

fn keyRec(k: u128) StreamID {
    return .{ .timestamp_ms = @truncate(k >> 64), .sequence = @truncate(k) };
}

const LGroup = struct {
    stream: usize,
    lease_ms: u64,
    max_attempts: u32,
    created_ns: u64,
    gen: u64,
    cursor: StreamID = StreamID.MIN,
    delivered: u64 = 0,
    settled: u64 = 0,
    released: u64 = 0,
    dead_lettered: u64 = 0,
    expired: u64 = 0,
    trimmed: u64 = 0,
    /// By record id.
    items: std.AutoArrayHashMapUnmanaged(u128, LItem) = .empty,

    fn heldBytes(g: *const LGroup) u64 {
        var sum: u64 = 0;
        for (g.items.values()) |v| sum += heldOf(v.err_len);
        return sum;
    }

    fn count(g: *const LGroup, s: Status) u64 {
        var n: u64 = 0;
        for (g.items.values()) |v| n += @intFromBool(v.status == s);
        return n;
    }
};

/// A stream as the ledger keeps it: whether the streams machine holds a
/// record of it (`made`; a tombstone holds one with `exists` false), its
/// floor and generation, and how many groups count it.
const LStream = struct { made: bool = false, exists: bool = false, floor: StreamID = StreamID.MIN, gen: u64 = 0, groups: u64 = 0, last: StreamID = StreamID.MIN };

/// An append the ledger counts: its first and last record ids.
const LAppend = struct { first: StreamID, last: StreamID, records: u32 };

const Ledger = struct {
    arena: std.heap.ArenaAllocator,
    /// [stream][group]: null while no meta exists.
    groups: [STREAMS.len][GROUPS.len]?LGroup = @splat(@splat(null)),
    streams: [STREAMS.len]LStream = @splat(.{}),
    /// Each stream's appends above its floor, oldest first.
    appends: [STREAMS.len]std.ArrayListUnmanaged(LAppend) = @splat(.empty),
    gpa: Allocator,

    fn init(a: Allocator) Ledger {
        return .{ .arena = std.heap.ArenaAllocator.init(a), .gpa = a };
    }

    fn deinit(self: *Ledger) void {
        self.arena.deinit();
        for (&self.appends) |*l| l.deinit(self.gpa);
    }

    fn reset(self: *Ledger) void {
        _ = self.arena.reset(.free_all);
        self.groups = @splat(@splat(null));
        self.streams = @splat(.{});
        for (&self.appends) |*l| l.clearRetainingCapacity();
    }

    fn alloc(self: *Ledger) Allocator {
        return self.arena.allocator();
    }

    fn live(self: *Ledger, s: usize, g: usize) bool {
        const grp = self.groups[s][g] orelse return false;
        return grp.gen == self.streams[s].gen;
    }

    fn addGroup(self: *Ledger, s: usize) *LStream {
        const st = &self.streams[s];
        st.groups += 1;
        return st;
    }

    fn dropGroup(self: *Ledger, s: usize) void {
        self.streams[s].groups -= 1;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// The family
// ═══════════════════════════════════════════════════════════════════════════

pub const ConsumerGroups = struct {
    /// The machine's: it resets this on every apply.
    scratch: std.heap.ArenaAllocator,
    /// The ledger's prediction for the entry applying.
    oracle: std.heap.ArenaAllocator,
    /// Reads of the streams machine's families, on the store applies
    /// hand in (`bind`).
    stream: stream_proj.StreamProjection = .{},
    ledger: Ledger,
    allocator: Allocator,

    pub const Kind = enum { append, trim, stream_delete, create, delete, deliver, settle, release, extend, expire, clean, steal, requeue, drop };

    /// What an applied entry did, beyond applying: the swarm fails if one
    /// never happens, since a rule it never met was never checked.
    pub const Mark = enum { deliver_leased, extend_moved, abandon, unread, nack_dead, nack_delay, nack_error, mixed_abandon_refused, cross_term_settle, cross_term_expire, steal_applied, requeue_applied, drop_applied, clean_gone, delete_refused };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        applied: u32 = 0,
        refused_items: u32 = 0,
        marks: std.EnumSet(Mark) = .empty,
        broke: ?[]const u8 = null,
    };

    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),
        items_applied: u64 = 0,
        items_refused: u64 = 0,
        marks: [std.enums.values(Mark).len]u64 = @splat(0),

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
            self.items_applied += o.applied;
            self.items_refused += o.refused_items;
            var it = o.marks.iterator();
            while (it.next()) |m| self.marks[@backingInt(m)] += 1;
        }

        pub fn add(self: *Counts, other: Counts) void {
            for (&self.applied, other.applied) |*a, b| a.* += b;
            for (&self.refused, other.refused) |*a, b| a.* += b;
            self.items_applied += other.items_applied;
            self.items_refused += other.items_refused;
            for (&self.marks, other.marks) |*a, b| a.* += b;
        }
    };

    pub fn init(allocator: Allocator) ConsumerGroups {
        return .{ .scratch = std.heap.ArenaAllocator.init(allocator), .oracle = std.heap.ArenaAllocator.init(allocator), .ledger = Ledger.init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *ConsumerGroups) void {
        self.scratch.deinit();
        self.oracle.deinit();
        self.ledger.deinit();
    }

    /// Replay re-applies from the start: the projection and the ledger go
    /// with the store the queue family clears.
    pub fn reset(self: *ConsumerGroups) void {
        self.stream = .{};
        self.ledger.reset();
    }

    pub fn owns(entry_type: u8) bool {
        for (gm.machine.entry_types) |t| if (@backingInt(t) == entry_type) return true;
        return entry_type == @backingInt(EntryType.stream_trim) or entry_type == @backingInt(EntryType.stream_delete) or entry_type == @backingInt(EntryType.stream_config);
    }

    /// The synthesized workload's appends share the type; this family's
    /// carry one of its streams' names and a record count. A synthesized
    /// payload leads with its op id, whose key-length bytes are zero.
    pub fn ownsEntry(entry_type: u8, payload: []const u8) bool {
        if (owns(entry_type)) return true;
        if (entry_type != @backingInt(EntryType.stream_append)) return false;
        const cmd = entry_mod.CommandPayload.deserialize(payload) orelse return false;
        return cmd.namespace_id == NS and streamIndex(cmd.key) != null and appendCount(cmd.value) != null and cmd.serializedSize() == payload.len;
    }

    /// Point reads at `store`, the one applies change.
    pub fn bind(self: *ConsumerGroups, store: *Store) void {
        self.stream.store = store;
        // Appends link by their stream's hint, as a server's do.
        store.setTailPrefix(sm.Records.ID, @sizeOf(gm.StreamKey));
    }

    pub fn streams(self: *const ConsumerGroups) streams_mod.Streams {
        return .{ .read = .{ .store = self.stream.store.? } };
    }

    fn kindOf(t: u8) Kind {
        return switch (@as(EntryType, @fromBackingInt(t))) {
            .stream_append => .append,
            .stream_trim => .trim,
            .stream_delete => .stream_delete,
            .group_create => .create,
            .group_delete => .delete,
            .group_deliver => .deliver,
            .group_settle => .settle,
            .group_release => .release,
            .group_extend => .extend,
            .group_expire => .expire,
            .group_steal => .steal,
            .group_requeue => .requeue,
            .group_drop => .drop,
            else => .clean,
        };
    }

    fn streamIndex(name: []const u8) ?usize {
        for (STREAMS, 0..) |s, i| if (std.mem.eql(u8, s, name)) return i;
        return null;
    }

    fn groupIndex(name: []const u8) ?usize {
        for (GROUPS, 0..) |g, i| if (std.mem.eql(u8, g, name)) return i;
        return null;
    }

    /// The (stream, group) a scope names, of the workload's.
    fn ofScope(scope: []const u8) ?[2]usize {
        for (STREAMS, 0..) |s, si| for (GROUPS, 0..) |g, gi| {
            if (std.mem.eql(u8, &gm.scopeOf(NS, s, g), scope)) return .{ si, gi };
        };
        return null;
    }

    // ── Store reads ─────────────────────────────────────────────────────

    pub fn meta(store: *Store, stream: []const u8, group: []const u8) ?gm.Meta.Value {
        var b: [4 + gm.SCOPE_LEN]u8 = undefined;
        const scope = gm.scopeOf(NS, stream, group);
        const got = (view_mod.ReadView{ .store = store }).get(gm.Metas, gm.storeKey(&b, NS, &scope)) catch return null;
        return if (got) |m| m.v else null;
    }

    pub fn item(store: *Store, stream: []const u8, group: []const u8, record: StreamID) ?gm.Item.Value {
        var b: [4 + gm.ID_LEN]u8 = undefined;
        const id = gm.idOf(gm.scopeOf(NS, stream, group), record);
        const got = (view_mod.ReadView{ .store = store }).get(gm.Items, gm.storeKey(&b, NS, &id)) catch return null;
        return if (got) |m| m.v else null;
    }

    pub fn streamRec(store: *Store, stream: []const u8) ?gm.StreamRec.Value {
        var b: [13]u8 = undefined;
        const k = gm.recKey(&b, gm.streamOf(NS, stream));
        const bytes = store.get(gm.Streams_.ID, k) orelse return null;
        return (gm.Streams_.Value.decode(bytes) catch return null).v;
    }

    fn shardHeld(store: *Store) u64 {
        const bytes = store.get(qm.Helds.ID, qm.HELD_KEY) orelse return 0;
        return (qm.Helds.Value.decode(bytes) catch return 0).v.bytes;
    }

    // ── Apply ───────────────────────────────────────────────────────────

    /// Apply `e` as the shard does, then check its answer and every group
    /// against the ledger, and the store's own rules. A refusal must leave
    /// every family as it was.
    pub fn apply(self: *ConsumerGroups, store: *Store, e: *const Entry, r: Replica) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        _ = self.oracle.reset(.retain_capacity);
        self.bind(store);
        const kind = kindOf(e.header.entry_type);
        var effect: Effect = .{ .kind = kind, .refused = false };
        switch (kind) {
            .append, .trim, .stream_delete => {
                effect.refused = try self.applyStream(store, e, kind, r, &effect);
                if (effect.broke == null) effect.broke = streamRule(store);
            },
            else => {
                // What the ledger says the answer must be, worked out before
                // the machine runs, from the ledger and the stream alone.
                const want = try self.predict(store, e, kind);
                const before = commits(store);
                // A mutant applies its own entry in the ledger's place.
                const applied = try self.rewritten(e, kind, r);
                var out: persistence.Result = .{};
                out.reset(&applied);
                try machine_mod.applyEntry(&gm.machine, store, &self.scratch, &applied, &out);
                if (out.refused()) |refused| {
                    if (!std.mem.eql(u64, &before, &commits(store))) return error.RefusalChangedState;
                    effect.refused = true;
                    if (want != .refused) {
                        effect.broke = "the group machine refused an entry the ledger applies";
                    } else if (want.refused != refused.status) {
                        effect.broke = "the group machine refused an entry with another status than the ledger's";
                    } else if (kind == .release and refused.status == .bad_request) {
                        effect.marks.insert(.mixed_abandon_refused);
                    } else if (kind == .delete) effect.marks.insert(.delete_refused);
                } else if (want == .refused) {
                    effect.broke = "the group machine applied an entry the ledger refuses";
                } else {
                    effect.broke = self.answerBroke(want, &out, &effect, e.header.term);
                    markOf(e, kind, want, &effect);
                    self.commit(e, kind, want);
                    try plantAfter(store, e, kind, want, r);
                }
            },
        }
        // Only the groups the entry could change are walked: every group at
        // every apply would cost the run its length squared.
        const touched = try touchedBy(e, kind);
        if (effect.broke == null) effect.broke = self.matchesLedger(store, touched);
        if (effect.broke == null) effect.broke = rulesOfStore(store, touched);
        return effect;
    }

    const Touched = [STREAMS.len][GROUPS.len]bool;

    /// The groups an entry names, or every group of the stream it names.
    fn touchedBy(e: *const Entry, kind: Kind) persistence.ApplyError!Touched {
        var t: Touched = @splat(@splat(false));
        var diag: schema.Diagnostic = .{};
        switch (kind) {
            .append, .trim, .stream_delete => {
                const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
                const s = if (kind == .trim) for (STREAMS, 0..) |name, i| {
                    if (std.mem.eql(u8, &streamHash(name), cmd.value)) break i;
                } else return error.Halt else streamIndex(cmd.key) orelse return error.Halt;
                t[s] = @splat(true);
            },
            .expire => {
                const p = ge.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| if (named.id.len == gm.ID_LEN) if (ofScope(named.id[0..gm.SCOPE_LEN])) |sg| {
                    t[sg[0]][sg[1]] = true;
                };
            },
            .clean => {
                const p = ge.Clean.decodeAll(e.payload, &diag) catch return error.Halt;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| if (ofScope(named.scope)) |sg| {
                    t[sg[0]][sg[1]] = true;
                };
            },
            inline else => |k| {
                const P = switch (k) {
                    .create => ge.Create,
                    .delete => ge.Delete,
                    .deliver => ge.Deliver,
                    .settle => ge.Settle,
                    .release => ge.Release,
                    .extend => ge.Extend,
                    .steal => ge.Steal,
                    .requeue, .drop => ge.DeadItems,
                    else => unreachable,
                };
                const p = P.decodeAll(e.payload, &diag) catch return error.Halt;
                t[streamIndex(p.stream) orelse return error.Halt][groupIndex(p.group) orelse return error.Halt] = true;
            },
        }
        return t;
    }

    fn commits(store: *const Store) [FAMILIES.len + 1]u64 {
        var out: [FAMILIES.len + 1]u64 = undefined;
        for (FAMILIES, out[0..FAMILIES.len]) |fam, *c| c.* = store.families[fam].commits;
        out[FAMILIES.len] = store.families[qm.Helds.ID].commits;
        return out;
    }

    /// A stream entry through the streams machine, as the shard applies it,
    /// and the ledger moved by its own rules: an append makes the stream, a
    /// trim moves its floor, and a delete (forced, or of a stream holding
    /// nothing above its floor) ends it, leaving a tombstone at the next
    /// generation while groups count it. Whether it was refused.
    fn applyStream(self: *ConsumerGroups, store: *Store, e: *const Entry, kind: Kind, r: Replica, effect: *Effect) (persistence.ApplyError || error{RefusalChangedState})!bool {
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
        const s: usize = if (kind == .trim) (for (STREAMS, 0..) |name, i| {
            if (cmd.value.len == 8 and std.mem.eql(u8, &streamHash(name), cmd.value)) break i;
        } else return error.Halt) else streamIndex(cmd.key) orelse return error.Halt;
        const sk = gm.streamOf(NS, STREAMS[s]);
        const st = &self.ledger.streams[s];
        // Whether a delete must be refused: the ledger's own appends above
        // its floor, never what the streams machine says it holds.
        const refuse = kind == .stream_delete and st.exists and e.header.flags & entry_mod.Flags.FORCE == 0 and self.ledger.appends[s].items.len > 0;
        const before = commits(store);
        var out: persistence.Result = .{};
        out.reset(e);
        const prev = if (sm.metaAt(.{ .store = store }, sk)) |m| m else null;
        try machine_mod.applyEntry(&sm.machine, store, &self.scratch, e, &out);
        if (out.refused()) |rf| {
            if (!std.mem.eql(u64, &before, &commits(store))) return error.RefusalChangedState;
            if (!refuse) effect.broke = "the streams machine refused a stream entry the ledger applies" else if (rf.status != .conflict) effect.broke = "the streams machine refused a delete with another status than the ledger's";
            return true;
        }
        if (refuse) effect.broke = "the streams machine deleted a stream holding records, unforced";
        switch (kind) {
            .append => {
                if (r.mutant == .stream_append_reuses_id) if (prev) |p| try restoreLast(store, sk, p);
                if (!st.exists) {
                    // Made, or made again from a tombstone, at its generation.
                    if (!st.made) st.gen = 0;
                    st.made = true;
                    st.exists = true;
                    st.floor = StreamID.MIN;
                    st.last = StreamID.MIN;
                }
            },
            .trim => {
                if (r.mutant == .stream_trim_leaves_floor) if (prev) |p| try restoreFloor(store, sk, p);
                // The floor moves to the last record of the last append the
                // bound reaches, never back.
                const bound = stream_proj.TrimBound.decode(cmd.key) orelse return error.Halt;
                if (st.exists) if (bound.resolve(e.header.timestamp_ns)) |to| if (lastReached(prev, self.ledgerAppends(s), to)) |cut| {
                    if (cut.greaterThan(st.floor)) st.floor = cut;
                };
                // What the floor cut is gone from the ledger's appends.
                const l = &self.ledger.appends[s];
                var keep: usize = 0;
                while (keep < l.items.len and !l.items[keep].last.greaterThan(st.floor)) keep += 1;
                l.replaceRange(self.ledger.gpa, 0, keep, &.{}) catch @panic("sim OOM");
            },
            else => if (st.exists) {
                st.exists = false;
                st.floor = StreamID.MIN;
                st.last = StreamID.MIN;
                if (st.groups > 0) {
                    // Its groups are of the old generation now: gone.
                    st.gen += 1;
                } else {
                    st.made = false;
                    st.gen = 0;
                }
                self.ledger.appends[s].clearRetainingCapacity();
            },
        }
        if (kind == .append and st.exists) {
            const n = appendCount(cmd.value) orelse return error.Halt;
            const ms = e.header.timestamp_ns / std.time.ns_per_ms;
            const last = st.last;
            const first: StreamID = if (ms > last.timestamp_ms) .{ .timestamp_ms = ms, .sequence = 0 } else .{ .timestamp_ms = last.timestamp_ms, .sequence = last.sequence + 1 };
            st.last = .{ .timestamp_ms = first.timestamp_ms, .sequence = first.sequence + n - 1 };
            self.ledger.appends[s].append(self.ledger.gpa, .{ .first = first, .last = st.last, .records = n }) catch @panic("sim OOM");
        }
        return false;
    }

    fn ledgerAppends(self: *ConsumerGroups, s: usize) []const LAppend {
        return self.ledger.appends[s].items;
    }

    /// The last record of the last append whose first id is at or before
    /// `to`, by the ledger's own appends.
    fn lastReached(_: ?sm.Meta.Value, appends: []const LAppend, to: StreamID) ?StreamID {
        var cut: ?StreamID = null;
        for (appends) |a| {
            if (a.first.greaterThan(to)) break;
            cut = a.last;
        }
        return cut;
    }

    /// `stream_append_reuses_id`: the stream's last id put back where it
    /// was, so the next append takes ids this one holds.
    fn restoreLast(store: *Store, sk: StreamKey, prev: sm.Meta.Value) persistence.ApplyError!void {
        const node = store.find(sm.Metas.ID, &sk) orelse return;
        var m = (sm.Metas.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
        m.last_ms = prev.last_ms;
        m.last_seq = prev.last_seq;
        try replaceMeta(store, node, m);
    }

    /// `stream_trim_leaves_floor`: the floor put back where it was.
    fn restoreFloor(store: *Store, sk: StreamKey, prev: sm.Meta.Value) persistence.ApplyError!void {
        const node = store.find(sm.Metas.ID, &sk) orelse return;
        var m = (sm.Metas.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
        m.floor_ms = prev.floor_ms;
        m.floor_seq = prev.floor_seq;
        try replaceMeta(store, node, m);
    }

    fn replaceMeta(store: *Store, node: *store_mod.Tree.Node, m: sm.Meta.Value) persistence.ApplyError!void {
        const bytes = try store.allocator.alloc(u8, sm.Meta.encodedSize(m) catch return error.Halt);
        _ = sm.Meta.encodeInto(bytes, m) catch return error.Halt;
        store.replace(sm.Metas.ID, node, bytes);
    }

    // ── What the ledger says an entry does ──────────────────────────────

    const Want = union(enum) {
        refused: @import("../protocol/proto.zig").StatusCode,
        create: struct { s: usize, g: usize, made: bool, lease_ms: u64, max_attempts: u32 },
        delete: struct { s: usize, g: usize, held: u64 },
        deliver: struct { s: usize, g: usize, made: bool, chosen: []const StreamID, deliveries: []const u32, fresh: usize },
        /// An extend's answer: the expiry its last applied record is left
        /// at, 0 when none applied.
        report: struct { s: usize, g: usize, applied: []const StreamID, refused: []const Refused, extend_to: u64 = 0, moved: bool = false, held: u32 = 0, cross_term: bool = false },
        expire: struct { ended: []const [3]u128, cross_term: bool },
        clean: []const Cleaned,
        steal: struct { s: usize, g: usize, taken: []const StreamID, deliveries: []const u32, refused: []const Refused, expiry_ns: u64 },
        dead: struct { s: usize, g: usize, applied: []const StreamID, refused: []const Refused },
    };
    const Refused = struct { record: StreamID, why: Why };
    /// A group's cleaned records, by (stream, group), and whether its meta goes.
    const Cleaned = struct { s: usize, g: usize, records: []const StreamID, drops_meta: bool, gone: bool };

    fn predict(self: *ConsumerGroups, store: *Store, e: *const Entry, kind: Kind) persistence.ApplyError!Want {
        var diag: schema.Diagnostic = .{};
        const L = &self.ledger;
        const a = self.oracle.allocator();
        const stamp = e.header.timestamp_ns;
        switch (kind) {
            .create => {
                const p = ge.Create.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = streamIndex(p.stream) orelse return error.Halt;
                const g = groupIndex(p.group) orelse return error.Halt;
                if (L.groups[s][g]) |grp| {
                    if (!L.live(s, g)) return .{ .refused = .unavailable };
                    if (grp.lease_ms != p.lease_ms or grp.max_attempts != p.max_attempts) return .{ .refused = .conflict };
                    return .{ .create = .{ .s = s, .g = g, .made = false, .lease_ms = p.lease_ms, .max_attempts = p.max_attempts } };
                }
                return .{ .create = .{ .s = s, .g = g, .made = true, .lease_ms = p.lease_ms, .max_attempts = p.max_attempts } };
            },
            .delete => {
                const p = ge.Delete.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = streamIndex(p.stream) orelse return error.Halt;
                const g = groupIndex(p.group) orelse return error.Halt;
                const grp = L.groups[s][g] orelse return .{ .refused = .not_found };
                if (!L.live(s, g)) return .{ .refused = .not_found };
                return .{ .delete = .{ .s = s, .g = g, .held = grp.items.count() } };
            },
            .deliver => return self.predictDeliver(store, e),
            .settle, .release, .extend => return self.predictReport(e, kind),
            .steal => {
                const p = ge.Steal.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = streamIndex(p.stream) orelse return error.Halt;
                const g = groupIndex(p.group) orelse return error.Halt;
                if (!L.live(s, g)) return .{ .refused = .not_found };
                const grp = &L.groups[s][g].?;
                const floor = L.streams[s].floor;
                var taken: std.ArrayListUnmanaged(StreamID) = .empty;
                var deliveries: std.ArrayListUnmanaged(u32) = .empty;
                var refused: std.ArrayListUnmanaged(Refused) = .empty;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| {
                    const rec: StreamID = .{ .timestamp_ms = named.ms, .sequence = named.seq };
                    // A trimmed record is cleanup's, not a steal's.
                    const found = if (rec.greaterThan(floor)) grp.items.get(recKey(rec)) else null;
                    const v = found orelse {
                        try refused.append(a, .{ .record = rec, .why = .gone });
                        continue;
                    };
                    // A record named twice: the workload names each once.
                    if (v.status != .leased or v.deliveries != named.deliveries) {
                        try refused.append(a, .{ .record = rec, .why = if (v.deliveries != named.deliveries) .delivered_again else switch (v.ended) {
                            .expired => .expired,
                            .released, .abandoned => .released,
                            else => .not_held,
                        } });
                        continue;
                    }
                    // Idle since its holder's last sign of life.
                    if (stamp -| v.touched_ns < p.min_idle_ms *| std.time.ns_per_ms) {
                        try refused.append(a, .{ .record = rec, .why = .not_idle });
                        continue;
                    }
                    try taken.append(a, rec);
                    try deliveries.append(a, v.deliveries +| 1);
                }
                return .{ .steal = .{ .s = s, .g = g, .taken = taken.items, .deliveries = deliveries.items, .refused = refused.items, .expiry_ns = afterMs(stamp, p.lease_ms) } };
            },
            .requeue, .drop => {
                const p = ge.DeadItems.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = streamIndex(p.stream) orelse return error.Halt;
                const g = groupIndex(p.group) orelse return error.Halt;
                if (!L.live(s, g)) return .{ .refused = .not_found };
                const grp = &L.groups[s][g].?;
                const floor = L.streams[s].floor;
                const sk = gm.streamOf(NS, p.stream);
                var applied: std.ArrayListUnmanaged(StreamID) = .empty;
                var refused: std.ArrayListUnmanaged(Refused) = .empty;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| {
                    const rec: StreamID = .{ .timestamp_ms = named.ms, .sequence = named.seq };
                    const v = grp.items.get(recKey(rec)) orelse {
                        try refused.append(a, .{ .record = rec, .why = .gone });
                        continue;
                    };
                    if (v.status != .dead) {
                        try refused.append(a, .{ .record = rec, .why = .not_dead });
                        continue;
                    }
                    // A trimmed record can't be read again: gone to a requeue.
                    if (kind == .requeue and (!rec.greaterThan(floor) or self.stream.streamAppendOf(sk, rec) == null)) {
                        try refused.append(a, .{ .record = rec, .why = .gone });
                        continue;
                    }
                    try applied.append(a, rec);
                }
                return .{ .dead = .{ .s = s, .g = g, .applied = applied.items, .refused = refused.items } };
            },
            .expire => {
                const p = ge.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var ended: std.ArrayListUnmanaged([3]u128) = .empty;
                var cross = false;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| {
                    if (named.expiry_ns > stamp or named.id.len != gm.ID_LEN) continue;
                    const sg = ofScope(named.id[0..gm.SCOPE_LEN]) orelse continue;
                    const grp = &(L.groups[sg[0]][sg[1]] orelse continue);
                    const rec = gm.recordOf(named.id);
                    const v = grp.items.get(recKey(rec)) orelse continue;
                    if (v.status != .leased or v.expiry_ns != named.expiry_ns) continue;
                    // One lease ends once, however often it is named.
                    if (for (ended.items) |x| {
                        if (x[0] == sg[0] and x[1] == sg[1] and x[2] == recKey(rec)) break true;
                    } else false) continue;
                    try ended.append(a, .{ sg[0], sg[1], recKey(rec) });
                    cross = cross or v.token.term < e.header.term;
                }
                return .{ .expire = .{ .ended = ended.items, .cross_term = cross } };
            },
            .clean => {
                const p = ge.Clean.decodeAll(e.payload, &diag) catch return error.Halt;
                var cleaned: std.ArrayListUnmanaged(Cleaned) = .empty;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| {
                    if (named.scope.len != gm.SCOPE_LEN) continue;
                    const sg = ofScope(named.scope) orelse continue;
                    const grp = L.groups[sg[0]][sg[1]] orelse continue;
                    // A scope named twice cleans what the first left.
                    var done: std.AutoHashMapUnmanaged(u128, void) = .empty;
                    var dropped = false;
                    for (cleaned.items) |c| if (c.s == sg[0] and c.g == sg[1]) {
                        for (c.records) |x| try done.put(a, recKey(x), {});
                        dropped = dropped or c.drops_meta;
                    };
                    if (dropped) continue;
                    const gone = !L.live(sg[0], sg[1]);
                    const floor = L.streams[sg[0]].floor;
                    var keys: std.ArrayListUnmanaged(u128) = .empty;
                    for (grp.items.keys()) |k| if (!done.contains(k)) try keys.append(a, k);
                    std.mem.sort(u128, keys.items, {}, std.sort.asc(u128));
                    var out: std.ArrayListUnmanaged(StreamID) = .empty;
                    var more = false;
                    for (keys.items) |k| {
                        if (out.items.len == ge.CLEAN_RECORDS) {
                            more = true;
                            break;
                        }
                        const rec = keyRec(k);
                        if (!gone and rec.greaterThan(floor)) break;
                        try out.append(a, rec);
                    }
                    try cleaned.append(a, .{ .s = sg[0], .g = sg[1], .records = out.items, .drops_meta = gone and !more, .gone = gone });
                }
                return .{ .clean = cleaned.items };
            },
            else => unreachable,
        }
    }

    fn predictDeliver(self: *ConsumerGroups, store: *Store, e: *const Entry) persistence.ApplyError!Want {
        var diag: schema.Diagnostic = .{};
        const L = &self.ledger;
        const a = self.oracle.allocator();
        const stamp = e.header.timestamp_ns;
        const p = ge.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
        const s = streamIndex(p.stream) orelse return error.Halt;
        const g = groupIndex(p.group) orelse return error.Halt;
        const sk = gm.streamOf(NS, p.stream);
        const made = L.groups[s][g] == null;
        if (made and !p.create) return .{ .refused = .not_found };
        if (!made and !L.live(s, g)) return .{ .refused = .unavailable };
        const floor = L.streams[s].floor;
        const grp: ?*LGroup = if (L.groups[s][g]) |*x| x else null;
        const cursor = if (grp) |x| x.cursor else StreamID.MIN;

        var chosen: std.ArrayListUnmanaged(StreamID) = .empty;
        // An answer counts each append once, at its bound; the first record
        // always fits.
        var seen: std.ArrayListUnmanaged(StreamID) = .empty;
        var used: u64 = 0;
        const Fit = enum { fits, full, gone };
        const fit = struct {
            fn f(proj: *const stream_proj.StreamProjection, key: StreamKey, record: StreamID, first: bool, sn: *std.ArrayListUnmanaged(StreamID), u: *u64, max: u64, al: Allocator) !Fit {
                const ap = proj.streamAppendOf(key, record) orelse return .gone;
                for (sn.items) |x| if (x.eql(ap.id)) return .fits;
                const bytes = answerBytes(ap.byte_len, ap.record_count);
                if (!first and u.* + bytes > max) return .full;
                u.* += bytes;
                try sn.append(al, ap.id);
                return .fits;
            }
        }.f;

        // The leader's chosen ready records, still ready and due, above the
        // floor, each once.
        var it = p.ready.iterator();
        while (it.next() catch return error.Halt) |named| {
            if (chosen.items.len == p.count) break;
            const rec: StreamID = .{ .timestamp_ms = named.ms, .sequence = named.seq };
            if (!rec.greaterThan(floor)) continue;
            if (for (chosen.items) |c| {
                if (c.eql(rec)) break true;
            } else false) continue;
            const x = grp orelse continue;
            const v = x.items.get(recKey(rec)) orelse continue;
            if (v.status != .ready or v.due_ns > stamp) continue;
            switch (try fit(&self.stream, sk, rec, chosen.items.len == 0, &seen, &used, p.max_bytes, a)) {
                .gone => continue,
                .full => break,
                .fits => try chosen.append(a, rec),
            }
        }
        // Then the stream's records past the cursor, up to the bound, while
        // the caps hold. A budget the ready ones filled still takes a fresh
        // record of an append already counted.
        var fresh: usize = 0;
        var capped = false;
        {
            const from = if (floor.greaterThan(cursor)) floor else cursor;
            const ids = try a.alloc(StreamID, p.count - chosen.items.len);
            const got = self.stream.streamIdsAfter(sk, from, ids);
            const held: u64 = if (grp) |x| x.items.count() else 0;
            const held_bytes: u64 = if (grp) |x| x.heldBytes() else 0;
            const shard = shardHeld(store);
            const each = heldOf(0);
            const through: StreamID = .{ .timestamp_ms = p.through_ms, .sequence = p.through_seq };
            for (ids[0..got]) |rec| {
                if (rec.greaterThan(through)) break;
                if (held + fresh + 1 > p.max_held or held_bytes + each * (fresh + 1) > p.max_held_bytes or shard + each * (fresh + 1) > p.shard_held_bytes) {
                    capped = true;
                    break;
                }
                switch (try fit(&self.stream, sk, rec, chosen.items.len == 0, &seen, &used, p.max_bytes, a)) {
                    .gone, .full => break,
                    .fits => {},
                }
                try chosen.append(a, rec);
                fresh += 1;
            }
        }
        if (chosen.items.len == 0 and capped) return .{ .refused = .overloaded };
        // Each one's deliveries after this one: a fresh record's first.
        const deliveries = try a.alloc(u32, chosen.items.len);
        for (chosen.items, deliveries) |rec, *d| {
            const before: u32 = if (grp) |x| (if (x.items.get(recKey(rec))) |v| v.deliveries else 0) else 0;
            d.* = before +| 1;
        }
        return .{ .deliver = .{ .s = s, .g = g, .made = made, .chosen = chosen.items, .deliveries = deliveries, .fresh = fresh } };
    }

    const ReportOf = struct { stream: []const u8, group: []const u8, holder: []const u8, token: ae.Token.Value, items: ge.Items.Value };

    fn reportOf(e: *const Entry, kind: Kind) persistence.ApplyError!ReportOf {
        var diag: schema.Diagnostic = .{};
        return switch (kind) {
            inline .settle, .release, .extend => |k| blk: {
                const P = switch (k) {
                    .settle => ge.Settle,
                    .release => ge.Release,
                    else => ge.Extend,
                };
                const p = P.decodeAll(e.payload, &diag) catch return error.Halt;
                break :blk .{ .stream = p.stream, .group = p.group, .holder = p.holder, .token = p.token, .items = p.items };
            },
            else => unreachable,
        };
    }

    fn holds(v: LItem, holder: []const u8, deliveries: u32, token: ae.Token.Value) bool {
        return v.status == .leased and same(v, holder, deliveries, token);
    }

    fn same(v: LItem, holder: []const u8, deliveries: u32, token: ae.Token.Value) bool {
        return v.deliveries == deliveries and v.token.term == token.term and v.token.index == token.index and std.mem.eql(u8, v.holder, holder);
    }

    fn whyNot(v: LItem, holder: []const u8, deliveries: u32, token: ae.Token.Value) Why {
        if (v.deliveries != deliveries or v.token.term != token.term or v.token.index != token.index) return .delivered_again;
        if (!std.mem.eql(u8, v.holder, holder)) return .other_holder;
        return switch (v.ended) {
            .expired => .expired,
            .released, .abandoned => .released,
            else => .not_held,
        };
    }

    fn predictReport(self: *ConsumerGroups, e: *const Entry, kind: Kind) persistence.ApplyError!Want {
        var diag: schema.Diagnostic = .{};
        const L = &self.ledger;
        const a = self.oracle.allocator();
        const c = try reportOf(e, kind);
        const s = streamIndex(c.stream) orelse return error.Halt;
        const g = groupIndex(c.group) orelse return error.Halt;
        if (kind == .release) {
            const p = ge.Release.decodeAll(e.payload, &diag) catch return error.Halt;
            if (p.ended != .failed and (p.dead or p.delay_ms > 0 or p.@"error".len > 0)) return .{ .refused = .bad_request };
        }
        if (!L.live(s, g)) return .{ .refused = .not_found };
        const grp = &L.groups[s][g].?;
        var applied: std.ArrayListUnmanaged(StreamID) = .empty;
        var refused: std.ArrayListUnmanaged(Refused) = .empty;
        var extend_to: u64 = 0;
        var moved = false;
        // Records the report holds the live delivery of, as opposed to a
        // resend counted as applied.
        var held: u32 = 0;
        var cross_term = false;
        var it = c.items.iterator();
        while (it.next() catch return error.Halt) |named| {
            const rec: StreamID = .{ .timestamp_ms = named.ms, .sequence = named.seq };
            const v = grp.items.get(recKey(rec)) orelse {
                try refused.append(a, .{ .record = rec, .why = .gone });
                continue;
            };
            // A record named twice in one report: its second finds what the
            // first left, which the ledger has not applied yet; the
            // workload names each once, so this stays simple.
            if (!holds(v, c.holder, named.deliveries, c.token)) {
                const repeat = kind == .release and same(v, c.holder, named.deliveries, c.token) and blk: {
                    const p = ge.Release.decodeAll(e.payload, &diag) catch return error.Halt;
                    break :blk v.ended == (if (p.ended == .failed) Ended.released else Ended.abandoned);
                };
                if (repeat) {
                    try applied.append(a, rec);
                    continue;
                }
                try refused.append(a, .{ .record = rec, .why = whyNot(v, c.holder, named.deliveries, c.token) });
                continue;
            }
            if (kind == .extend) {
                const p = ge.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
                if (p.lease_ms != v.lease_ms or p.held_since_ns != v.delivered_ns) {
                    try refused.append(a, .{ .record = rec, .why = .delivered_again });
                    continue;
                }
                // More than half the lease left, or held to the ceiling: it
                // stands, and the answer says where it ends.
                const stamp = e.header.timestamp_ns;
                const to = extendTo(stamp, v.lease_ms, v.delivered_ns);
                const stands = v.expiry_ns -| stamp > (v.lease_ms *| std.time.ns_per_ms) / 2 or to <= v.expiry_ns;
                extend_to = if (stands) v.expiry_ns else to;
                moved = moved or !stands;
            }
            held += 1;
            cross_term = cross_term or v.token.term < e.header.term;
            try applied.append(a, rec);
        }
        return .{ .report = .{ .s = s, .g = g, .applied = applied.items, .refused = refused.items, .extend_to = extend_to, .moved = moved, .held = held, .cross_term = cross_term } };
    }

    /// Whether the machine's answer is the ledger's.
    /// Each refused record's id and reason, then nothing more.
    fn refusedBroke(rd: anytype, refused: []const Refused) ?[]const u8 {
        for (refused) |x| {
            const ms = rd.int(u64) orelse return "an entry refused fewer records than the ledger";
            const seq = rd.int(u64) orelse return "an entry refused fewer records than the ledger";
            const why: Why = @fromBackingInt(rd.int(u8) orelse return "an entry refused fewer records than the ledger");
            if (ms != x.record.timestamp_ms or seq != x.record.sequence) return "an entry refused other records than the ledger";
            if (why != x.why) return "an entry refused a record with another reason than the ledger's";
        }
        if (rd.int(u64) != null) return "an entry refused more records than the ledger";
        return null;
    }

    fn answerBroke(self: *ConsumerGroups, want: Want, out: *const persistence.Result, effect: *Effect, term: u64) ?[]const u8 {
        _ = self;
        var rd = out.reader();
        switch (want) {
            .refused => unreachable,
            .create => |w| {
                const made = rd.int(u8) orelse return "a create answered nothing";
                if ((made == 1) != w.made) return "a create answered made where the ledger has the group, or the reverse";
            },
            .delete => |w| {
                const held = rd.int(u64) orelse return "a delete answered nothing";
                if (held != w.held) return "a delete answered another count of held records than the ledger's";
            },
            .deliver => |w| {
                const n = rd.int(u32) orelse return "a deliver answered nothing";
                if (n != w.chosen.len) return "a deliver leased another number of records than the ledger's choice";
                for (w.chosen, w.deliveries) |c, d| {
                    const ms = rd.int(u64) orelse return "a deliver's answer is short";
                    const seq = rd.int(u64) orelse return "a deliver's answer is short";
                    const got = rd.int(u32) orelse return "a deliver's answer is short";
                    if (ms != c.timestamp_ms or seq != c.sequence) return "a deliver leased other records than the ledger's choice, or in another order";
                    if (got != d) return "a deliver answered a record's deliveries otherwise than the ledger";
                }
                effect.applied = n;
            },
            .report => |w| {
                const n = rd.int(u32) orelse return "a report answered nothing";
                if (n != w.applied.len) return "a report applied to another number of records than the ledger's";
                if (effect.kind == .extend) {
                    const to = rd.int(u64) orelse return "an extend answered no expiry";
                    if (to != w.extend_to) return "an extend answered another expiry than the ledger's";
                }
                for (w.refused) |x| {
                    const ms = rd.int(u64) orelse return "a report refused fewer records than the ledger";
                    const seq = rd.int(u64) orelse return "a report refused fewer records than the ledger";
                    const why: Why = @fromBackingInt(rd.int(u8) orelse return "a report refused fewer records than the ledger");
                    if (ms != x.record.timestamp_ms or seq != x.record.sequence) return "a report refused other records than the ledger";
                    if (why != x.why) return "a report refused a record with another reason than the ledger's";
                }
                if (rd.int(u64) != null) return "a report refused more records than the ledger";
                effect.applied = n;
                effect.refused_items = @intCast(w.refused.len);
            },
            .expire => |w| {
                const n = rd.int(u32) orelse return "an expire answered nothing";
                if (n != w.ended.len) return "an expire ended another number of leases than the ledger's";
                effect.applied = n;
            },
            .steal => |w| {
                const term_answered = rd.int(u64) orelse return "a steal answered nothing";
                const expiry = rd.int(u64) orelse return "a steal answered nothing";
                const n = rd.int(u32) orelse return "a steal answered nothing";
                if (term_answered != term) return "a steal answered another term than its entry's";
                if (expiry != w.expiry_ns) return "a steal answered another expiry than the ledger's";
                if (n != w.taken.len) return "a steal took another number of records than the ledger";
                for (w.taken, w.deliveries) |c, d| {
                    const ms = rd.int(u64) orelse return "a steal's answer is short";
                    const seq = rd.int(u64) orelse return "a steal's answer is short";
                    const got = rd.int(u32) orelse return "a steal's answer is short";
                    if (ms != c.timestamp_ms or seq != c.sequence) return "a steal took other records than the ledger";
                    if (got != d) return "a steal answered a record's deliveries otherwise than the ledger";
                }
                if (refusedBroke(&rd, w.refused)) |why| return why;
                effect.applied = n;
                effect.refused_items = @intCast(w.refused.len);
            },
            .dead => |w| {
                const n = rd.int(u32) orelse return "a requeue or drop answered nothing";
                if (n != w.applied.len) return "a requeue or drop applied to another number of records than the ledger";
                if (refusedBroke(&rd, w.refused)) |why| return why;
                effect.applied = n;
                effect.refused_items = @intCast(w.refused.len);
            },
            .clean => |w| {
                const n = rd.int(u64) orelse return "a clean answered nothing";
                var sum: u64 = 0;
                for (w) |c| sum += c.records.len;
                if (n != sum) return "a clean removed another number of records than the ledger's";
            },
        }
        return null;
    }

    /// The ledger moves as the entry says, by its own rules.
    fn commit(self: *ConsumerGroups, e: *const Entry, kind: Kind, want: Want) void {
        var diag: schema.Diagnostic = .{};
        const L = &self.ledger;
        const la = L.alloc();
        const stamp = e.header.timestamp_ns;
        const token: ae.Token.Value = .{ .term = e.header.term, .index = e.header.index };
        switch (want) {
            .refused => unreachable,
            .create => |w| if (w.made) {
                const st = L.addGroup(w.s);
                L.groups[w.s][w.g] = .{ .stream = w.s, .lease_ms = w.lease_ms, .max_attempts = w.max_attempts, .created_ns = stamp, .gen = st.gen };
            },
            .delete => |w| {
                const grp = &L.groups[w.s][w.g].?;
                if (w.held == 0) {
                    L.groups[w.s][w.g] = null;
                    L.dropGroup(w.s);
                } else grp.gen = gm.GONE;
            },
            .deliver => |w| {
                const p = ge.Deliver.decodeAll(e.payload, &diag) catch unreachable;
                if (w.made) {
                    const st = L.addGroup(w.s);
                    L.groups[w.s][w.g] = .{ .stream = w.s, .lease_ms = ge.DEFAULT_LEASE_MS, .max_attempts = ge.DEFAULT_MAX_ATTEMPTS, .created_ns = stamp, .gen = st.gen };
                }
                const grp = &L.groups[w.s][w.g].?;
                const holder = la.dupe(u8, p.holder) catch @panic("sim OOM");
                for (w.chosen, 0..) |rec, i| {
                    const fresh = i >= w.chosen.len - w.fresh;
                    const gop = grp.items.getOrPut(la, recKey(rec)) catch @panic("sim OOM");
                    if (fresh) gop.value_ptr.* = .{ .status = .ready, .due_ns = stamp };
                    const v = gop.value_ptr;
                    v.status = .leased;
                    v.deliveries +|= 1;
                    v.holder = holder;
                    v.token = token;
                    v.lease_ms = p.lease_ms;
                    v.delivered_ns = stamp;
                    v.expiry_ns = afterMs(stamp, p.lease_ms);
                    v.ended = .none;
                    v.touched_ns = stamp;
                    if (v.started_ns == 0) v.started_ns = stamp;
                }
                if (w.fresh > 0) grp.cursor = w.chosen[w.chosen.len - 1];
                grp.delivered += w.chosen.len;
            },
            .report => |w| {
                const grp = &L.groups[w.s][w.g].?;
                const c = reportOf(e, kind) catch unreachable;
                for (w.applied) |rec| {
                    const v = grp.items.getPtr(recKey(rec)).?;
                    // A resent release changes nothing.
                    if (!holds(v.*, c.holder, v.deliveries, c.token) or v.status != .leased) continue;
                    switch (kind) {
                        .settle => {
                            _ = grp.items.orderedRemove(recKey(rec));
                            grp.settled += 1;
                        },
                        .release => {
                            const p = ge.Release.decodeAll(e.payload, &diag) catch unreachable;
                            if (p.ended != .failed) {
                                v.ended = .abandoned;
                                if (p.ended == .unread) v.deliveries -= 1;
                                v.status = .ready;
                                v.due_ns = stamp;
                                continue;
                            }
                            v.ended = .released;
                            v.failures +|= 1;
                            v.err_len = p.@"error".len;
                            grp.released += 1;
                            if (p.dead or v.failures >= grp.max_attempts) {
                                die(grp, v, stamp);
                            } else {
                                v.status = .ready;
                                v.due_ns = afterMs(stamp, p.delay_ms);
                            }
                        },
                        else => {
                            // A touch is a sign of life, whether or not it
                            // moves the lease.
                            v.touched_ns = stamp;
                            if (v.expiry_ns -| stamp > (v.lease_ms *| std.time.ns_per_ms) / 2) continue;
                            const to = extendTo(stamp, v.lease_ms, v.delivered_ns);
                            if (to > v.expiry_ns) v.expiry_ns = to;
                        },
                    }
                }
            },
            .expire => |w| for (w.ended) |x| {
                const grp = &L.groups[@intCast(x[0])][@intCast(x[1])].?;
                const v = grp.items.getPtr(x[2]).?;
                v.ended = .expired;
                v.failures +|= 1;
                v.err_len = "lease expired".len;
                grp.expired += 1;
                if (v.failures >= grp.max_attempts) die(grp, v, stamp) else {
                    v.status = .ready;
                    v.due_ns = stamp;
                }
            },
            .steal => |w| {
                const p = ge.Steal.decodeAll(e.payload, &diag) catch unreachable;
                const grp = &L.groups[w.s][w.g].?;
                const holder = la.dupe(u8, p.holder) catch @panic("sim OOM");
                for (w.taken) |rec| {
                    const v = grp.items.getPtr(recKey(rec)).?;
                    v.deliveries +|= 1;
                    v.holder = holder;
                    v.token = token;
                    v.lease_ms = p.lease_ms;
                    v.delivered_ns = stamp;
                    v.expiry_ns = afterMs(stamp, p.lease_ms);
                    v.ended = .none;
                    v.touched_ns = stamp;
                }
                grp.delivered += w.taken.len;
            },
            .dead => |w| {
                const grp = &L.groups[w.s][w.g].?;
                for (w.applied) |rec| {
                    if (kind == .drop) {
                        _ = grp.items.orderedRemove(recKey(rec));
                        continue;
                    }
                    const v = grp.items.getPtr(recKey(rec)).?;
                    v.status = .ready;
                    v.due_ns = stamp;
                    v.failures = 0;
                    v.dead_ns = 0;
                }
            },
            .clean => |w| for (w) |c| {
                const grp = &L.groups[c.s][c.g].?;
                const gone = !L.live(c.s, c.g);
                for (c.records) |rec| _ = grp.items.orderedRemove(recKey(rec));
                if (!gone) grp.trimmed += c.records.len;
                if (c.drops_meta) {
                    L.groups[c.s][c.g] = null;
                    L.dropGroup(c.s);
                }
            },
        }
    }

    // ── Mutants ─────────────────────────────────────────────────────────

    /// The entry a mutant applies in place of `e`; `e` itself otherwise.
    fn rewritten(self: *ConsumerGroups, e: *const Entry, kind: Kind, r: Replica) persistence.ApplyError!Entry {
        var out = e.*;
        var diag: schema.Diagnostic = .{};
        const a = self.oracle.allocator();
        switch (r.mutant) {
            .group_deliver_cursor_first => if (kind == .deliver) {
                var p = ge.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
                p.ready = .{ .bytes = "", .count = 0 };
                const buf = try a.alloc(u8, ge.Deliver.encodedSize(p) catch return error.Halt);
                out.payload = ge.Deliver.encodeInto(buf, p) catch return error.Halt;
            },
            .group_abandon_counts_failure => if (kind == .release) {
                var p = ge.Release.decodeAll(e.payload, &diag) catch return error.Halt;
                if (p.ended != .abandoned) return out;
                p.ended = .failed;
                const buf = try a.alloc(u8, ge.Release.encodedSize(p) catch return error.Halt);
                out.payload = ge.Release.encodeInto(buf, p) catch return error.Halt;
            },
            .group_steal_ignores_idle => if (kind == .steal) {
                // Idle a millisecond is idle enough: a busy holder is robbed.
                var p = ge.Steal.decodeAll(e.payload, &diag) catch return error.Halt;
                p.min_idle_ms = 1;
                const buf = try a.alloc(u8, ge.Steal.encodedSize(p) catch return error.Halt);
                out.payload = ge.Steal.encodeInto(buf, p) catch return error.Halt;
            },
            .group_extend_clock_ahead => if (kind == .extend) {
                const p = ge.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
                out.header.timestamp_ns +|= (p.lease_ms *| std.time.ns_per_ms) / 2 + std.time.ns_per_ms;
            },
            .group_expire_ignores_deadline => if (kind == .expire) {
                // As late as its latest named live lease: one not yet due
                // then ends.
                const p = ge.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |named| {
                    if (named.id.len != gm.ID_LEN) continue;
                    const sg = ofScope(named.id[0..gm.SCOPE_LEN]) orelse continue;
                    const grp = self.ledger.groups[sg[0]][sg[1]] orelse continue;
                    const v = grp.items.get(recKey(gm.recordOf(named.id))) orelse continue;
                    if (v.status == .leased and v.expiry_ns == named.expiry_ns) out.header.timestamp_ns = @max(out.header.timestamp_ns, named.expiry_ns);
                }
            },
            else => {},
        }
        return out;
    }

    /// A mutant's change to the state an applied entry left.
    fn plantAfter(store: *Store, e: *const Entry, kind: Kind, want: Want, r: Replica) persistence.ApplyError!void {
        switch (r.mutant) {
            .group_settle_keeps_bytes => if (kind == .settle) {
                const w = want.report;
                if (w.applied.len == 0) return;
                var b: [4 + gm.SCOPE_LEN]u8 = undefined;
                const scope = gm.scopeOf(NS, STREAMS[w.s], GROUPS[w.g]);
                const key = gm.storeKey(&b, NS, &scope);
                const node = store.find(gm.Metas.ID, key) orelse return;
                var m = (gm.Metas.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
                m.held_bytes += heldOf(0) * w.applied.len;
                const bytes = try store.allocator.alloc(u8, gm.Meta.encodedSize(m) catch return error.Halt);
                _ = gm.Meta.encodeInto(bytes, m) catch return error.Halt;
                store.replace(gm.Metas.ID, node, bytes);
            },
            .group_requeue_keeps_failures => if (kind == .requeue) {
                // A requeued record keeps one failure: its attempts aren't reset.
                const w = want.dead;
                for (w.applied) |rec| {
                    var b: [4 + gm.ID_LEN]u8 = undefined;
                    const id = gm.idOf(gm.scopeOf(NS, STREAMS[w.s], GROUPS[w.g]), rec);
                    const key = gm.storeKey(&b, NS, &id);
                    const node = store.find(gm.Items.ID, key) orelse continue;
                    var v = (gm.Items.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
                    v.failures = 1;
                    const bytes = try store.allocator.alloc(u8, gm.Item.encodedSize(v) catch return error.Halt);
                    _ = gm.Item.encodeInto(bytes, v) catch return error.Halt;
                    store.replace(gm.Items.ID, node, bytes);
                }
            },
            .group_drop_keeps_bytes => if (kind == .drop) {
                const w = want.dead;
                if (w.applied.len == 0) return;
                var b: [4 + gm.SCOPE_LEN]u8 = undefined;
                const scope = gm.scopeOf(NS, STREAMS[w.s], GROUPS[w.g]);
                const node = store.find(gm.Metas.ID, gm.storeKey(&b, NS, &scope)) orelse return;
                var m = (gm.Metas.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
                m.held_bytes += heldOf(0) * w.applied.len;
                const bytes = try store.allocator.alloc(u8, gm.Meta.encodedSize(m) catch return error.Halt);
                _ = gm.Meta.encodeInto(bytes, m) catch return error.Halt;
                store.replace(gm.Metas.ID, node, bytes);
            },
            .group_delete_keeps_count => if (kind == .delete) {
                var b: [13]u8 = undefined;
                const key = gm.recKey(&b, gm.streamOf(NS, STREAMS[want.delete.s]));
                const node = store.find(gm.Streams_.ID, key) orelse return;
                var rec = (gm.Streams_.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
                rec.groups += 1;
                const bytes = try store.allocator.alloc(u8, gm.StreamRec.encodedSize(rec) catch return error.Halt);
                _ = gm.StreamRec.encodeInto(bytes, rec) catch return error.Halt;
                store.replace(gm.Streams_.ID, node, bytes);
            },
            else => {},
        }
        _ = e;
    }

    /// The effects an applied entry had, by the ledger's prediction.
    fn markOf(e: *const Entry, kind: Kind, want: Want, effect: *Effect) void {
        var diag: schema.Diagnostic = .{};
        switch (want) {
            .deliver => |w| if (w.chosen.len > 0) effect.marks.insert(.deliver_leased),
            .steal => |w| if (w.taken.len > 0) effect.marks.insert(.steal_applied),
            .dead => |w| if (w.applied.len > 0) effect.marks.insert(if (kind == .requeue) .requeue_applied else .drop_applied),
            .clean => |w| for (w) |c| if (c.drops_meta or (c.gone and c.records.len > 0)) effect.marks.insert(.clean_gone),
            .expire => |w| if (w.cross_term) effect.marks.insert(.cross_term_expire),
            .report => |w| {
                if (w.held == 0) return;
                switch (kind) {
                    .settle => if (w.cross_term) effect.marks.insert(.cross_term_settle),
                    .extend => if (w.moved) effect.marks.insert(.extend_moved),
                    .release => {
                        const p = ge.Release.decodeAll(e.payload, &diag) catch return;
                        switch (p.ended) {
                            .abandoned => effect.marks.insert(.abandon),
                            .unread => effect.marks.insert(.unread),
                            .failed => {
                                if (p.dead) effect.marks.insert(.nack_dead);
                                if (p.delay_ms > 0) effect.marks.insert(.nack_delay);
                                if (p.@"error".len > 0) effect.marks.insert(.nack_error);
                            },
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
    }

    fn die(grp: *LGroup, v: *LItem, stamp: u64) void {
        v.status = .dead;
        v.dead_ns = stamp;
        grp.dead_lettered += 1;
    }

    // ── The ledger against the store ────────────────────────────────────

    fn matchesLedger(self: *ConsumerGroups, store: *Store, touched: Touched) ?[]const u8 {
        const L = &self.ledger;
        for (STREAMS, 0..) |sname, s| {
            if (std.mem.indexOfScalar(bool, &touched[s], true) == null) continue;
            const st = L.streams[s];
            const view = gm.streamViewOf(.{ .store = store }, gm.streamOf(NS, sname));
            if (view.gen != st.gen or view.groups != st.groups) return "a stream's record has another generation or group count than the ledger's";
            if (view.floor_ms != st.floor.timestamp_ms or view.floor_seq != st.floor.sequence) return "a stream's floor is not the last record it lost";
            if ((streamRec(store, sname) != null) != (st.groups > 0)) return "a stream's group record is kept where no group counts it, or gone where one does";
            for (GROUPS, 0..) |gname, g| {
                if (!touched[s][g]) continue;
                const m = meta(store, sname, gname);
                const grp = L.groups[s][g] orelse {
                    if (m != null) return "a group's meta stays where the ledger has none";
                    continue;
                };
                const got = m orelse return "a group's meta is gone where the ledger keeps it";
                if ((got.gen == gm.GONE) != (grp.gen == gm.GONE)) return "a group is gone where the ledger keeps it live, or the reverse";
                if (grp.gen != gm.GONE and got.gen != grp.gen) return "a group's generation is not its stream's at its create";
                if (got.lease_ms != grp.lease_ms or got.max_attempts != grp.max_attempts) return "a group's settings are not those it was made with";
                if (got.cursor_ms != grp.cursor.timestamp_ms or got.cursor_seq != grp.cursor.sequence) return "a group's cursor is not the last fresh record it delivered";
                if (got.ready != grp.count(.ready) or got.leased != grp.count(.leased) or got.dead != grp.count(.dead)) return "a group's counts are not the ledger's";
                if (got.held_bytes != grp.heldBytes()) return "a group's held bytes are not what the ledger's records hold";
                if (got.delivered != grp.delivered or got.settled != grp.settled or got.released != grp.released or got.dead_lettered != grp.dead_lettered or got.expired != grp.expired or got.trimmed != grp.trimmed)
                    return "a group's counters are not the ledger's";
                // Each of the ledger's records, as the store holds it.
                for (grp.items.keys(), grp.items.values()) |k, v| {
                    const it = item(store, sname, gname, keyRec(k)) orelse return "a record the ledger holds is gone from its group";
                    if (it.status != v.status) return "a record's state is not the ledger's";
                    if (it.deliveries != v.deliveries or it.failures != v.failures) return "a record's deliveries or failures are not the ledger's";
                    if (it.ended != v.ended or it.@"error".len != v.err_len) return "a record's ending is not the ledger's";
                    switch (v.status) {
                        .ready => if (it.due_ns != v.due_ns) return "a ready record is due at another time than the ledger's",
                        .leased => if (!std.mem.eql(u8, it.holder, v.holder) or it.token.term != v.token.term or it.token.index != v.token.index or it.expiry_ns != v.expiry_ns or it.lease_ms != v.lease_ms or it.delivered_ns != v.delivered_ns or it.touched_ns != v.touched_ns)
                            return "a leased record's delivery is not the ledger's",
                        .dead => if (it.dead_ns != v.dead_ns) return "a dead record died at another time than the ledger's",
                    }
                }
                // And no record the ledger doesn't hold.
                var n: u64 = 0;
                var pb: [4 + gm.SCOPE_LEN]u8 = undefined;
                const scope = gm.scopeOf(NS, sname, gname);
                var it = (view_mod.ReadView{ .store = store }).range(gm.Items, gm.storeKey(&pb, NS, &scope));
                while (it.next() catch null) |_| n += 1;
                if (n != grp.items.count()) return "a group holds a record the ledger doesn't";
            }
        }
        return null;
    }

    /// What any store of streams must be: a stream's appends rise, each past
    /// the last; its last id is its last append's; its counts above the
    /// floor are its appends' there; it keeps appends at or below its floor
    /// only while marked to reclaim them; and a stream the machine holds no
    /// record of, or a tombstone, has none.
    fn streamRule(store: *Store) ?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = store };
        for (STREAMS) |name| {
            const sk = gm.streamOf(NS, name);
            const m = sm.metaAt(rv, sk);
            var it = rv.range(sm.Records, &sk);
            const live = if (m) |x| x.exists else false;
            if (!live) {
                if ((it.next() catch null) != null) return "a stream that doesn't exist holds appends";
                continue;
            }
            const sm_meta = m.?;
            const floor = sm.floorOf(sm_meta);
            var prev_last = StreamID.MIN;
            var any = false;
            var below = false;
            var above: u64 = 0;
            while (it.next() catch null) |rec_item| {
                const rec = sm.recordOf(rec_item.key, rec_item.value.v);
                if (any and !rec.id.greaterThan(prev_last)) return "a stream's appends overlap: an id was handed out twice";
                any = true;
                prev_last = stream_proj.lastRecordOf(rec);
                if (prev_last.greaterThan(floor)) above += rec.record_count else below = true;
            }
            if (any and !prev_last.eql(sm.lastOf(sm_meta))) return "a stream's last id is not its last append's";
            if (above != sm.countAbove(rv, sk, sm_meta, floor).records) return "a stream's count above its floor is not its appends'";
            if (below != (store.get(sm.Reclaim.ID, &sk) != null)) return "a stream keeps appends at or below its floor without being marked to reclaim them, or is marked with none";
        }
        return null;
    }

    /// What any store of groups must be: each record has the key its state
    /// says, no ready or dead key is left over, and the shard's held bytes
    /// are every queue's and every group's.
    fn rulesOfStore(store: *Store, touched: Touched) ?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = store };
        for (STREAMS, 0..) |sname, si| for (GROUPS, 0..) |gname, gi| {
            if (!touched[si][gi]) continue;
            const scope = gm.scopeOf(NS, sname, gname);
            var pb: [4 + gm.SCOPE_LEN]u8 = undefined;
            var counts: [3]u64 = .{ 0, 0, 0 };
            var it = rv.range(gm.Items, gm.storeKey(&pb, NS, &scope));
            while (it.next() catch null) |entry| {
                const v = entry.value.v;
                const id = entry.key[4..];
                counts[@backingInt(v.status)] += 1;
                var b: KeyBuf = undefined;
                var kb: [4 + @sizeOf(KeyBuf)]u8 = undefined;
                switch (v.status) {
                    .ready => if (store.get(gm.Ready.ID, gm.storeKey(&kb, NS, gm.readyKeyOf(&b, v, v.due_ns, id))) == null) return "a ready record has no ready key at its due time",
                    .leased => if (store.get(gm.Deadlines.ID, view_mod.key(&kb, "", gm.deadlineKey(&b, v.expiry_ns, NS, id)) catch return null) == null) return "a leased record has no deadline key at its expiry",
                    .dead => {
                        const dk = gm.deadKey(scope, v.dead_ns, gm.recordOf(id));
                        if (store.get(gm.Dead.ID, gm.storeKey(&kb, NS, &dk)) == null) return "a dead record has no dead key";
                    },
                }
            }
            var rb: [gm.SCOPE_LEN + 1]u8 = undefined;
            var rk: [4 + gm.SCOPE_LEN + 1]u8 = undefined;
            var ready_keys: u64 = 0;
            var ri = rv.range(gm.Ready, gm.storeKey(&rk, NS, gm.readyPrefix(&rb, &scope)));
            while (ri.next() catch null) |_| ready_keys += 1;
            if (ready_keys != counts[0]) return "a group has a ready key no ready record holds";
            var dead_keys: u64 = 0;
            var di = rv.range(gm.Dead, gm.storeKey(&pb, NS, &scope));
            while (di.next() catch null) |_| dead_keys += 1;
            if (dead_keys != counts[2]) return "a group has a dead key no dead record holds";
        };
        var sum: u64 = 0;
        var qi = rv.range(qm.Metas, "");
        while (qi.next() catch null) |m| sum += m.value.v.held_bytes;
        var gi = rv.range(gm.Metas, "");
        while (gi.next() catch null) |m| sum += m.value.v.held_bytes;
        if (sum != shardHeld(store)) return "the shard's held bytes aren't its queues' and groups' held bytes";
        return null;
    }

    pub fn checksums(self: *ConsumerGroups, store: *Store) [SUMS]u64 {
        var sums: [SUMS]u64 = undefined;
        for (FAMILIES, sums[0..FAMILIES.len]) |fam, *sum| sum.* = store_mod.checksum(store, fam);
        // The projection's view of each stream, as the machine reads it.
        var h = std.hash.Wyhash.init(0x57);
        for (STREAMS) |name| {
            const sk = gm.streamOf(NS, name);
            for ([_]StreamID{ self.stream.streamFirstId(sk), self.stream.streamLastId(sk), self.stream.streamRemovedThrough(sk) }) |id| h.update(std.mem.asBytes(&id));
        }
        sums[FAMILIES.len] = h.final();
        return sums;
    }
};
