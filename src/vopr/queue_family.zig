//! The queue machine on a store of its own, as `families.Actions` is for
//! actions: every replica applies the same entries through the server's own
//! machine (`queue/machine.zig`), the checker compares the families'
//! checksums, and each entry is checked against its queue's messages before
//! and after it applies:
//!
//! - a deliver leases the first due ready messages in (priority, due, seq)
//!   order, to the holder and token it names, for its lease;
//! - settle, release and extend change a message only for the live
//!   delivery they name; any other message they name is left as it was;
//! - deliveries and failures never go down; a message is dead exactly when
//!   its failures reach the cap or a release names it dead;
//! - an expire ends only a lease whose deadline it names, past due;
//! - every ready, leased and dead message has its key, and a queue's
//!   counts are what its families hold;
//! - a purge leaves its queue empty;
//! - an enqueue is refused exactly when its queue's messages or payload
//!   bytes, or its shard's, would pass the caps the entry carries, and
//!   one that applies leaves them within those caps;
//! - a queue's held bytes are its messages' payloads, and the shard's are
//!   every queue's.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const view_mod = @import("../contract/view.zig");
const schema = @import("../protocol/schema.zig");
const qe = @import("../protocol/queue_entries.zig");
const ae = @import("../protocol/action_entries.zig");
const qm = @import("../queue/machine.zig");
const families = @import("families.zig");

const Allocator = std.mem.Allocator;
const Msg = qm.Msg.Value;
const NamespaceId = qm.NamespaceId;
const Replica = families.Replica;

pub const Queues = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    /// A mutant's rewritten payload.
    rewrite: []u8,

    pub const FAMILIES = [_]u16{ qm.Metas.ID, qm.Msgs.ID, qm.Dead.ID, qm.Ready.ID, qm.Deadlines.ID, qm.Helds.ID };
    pub const NAMES = [_][]const u8{ "queue.meta", "queue.msg", "queue.dead", "queue.ready", "queue.deadline", "queue.held" };

    pub const Kind = enum { enqueue, deliver, settle, release, extend, expire, purge };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        /// Messages the entry applied to, or refused one by one.
        applied: u32 = 0,
        refused_items: u32 = 0,
        /// A settle, or an expire ending a lease, of a delivery made in an
        /// earlier term.
        cross_term: bool = false,
        broke: ?[]const u8 = null,
    };

    /// Entries by kind and outcome, each counted once, and the messages
    /// reports applied to and refused.
    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),
        items_applied: u64 = 0,
        items_refused: u64 = 0,
        cross_term_settles: u64 = 0,
        cross_term_expires: u64 = 0,

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
            self.items_applied += o.applied;
            self.items_refused += o.refused_items;
            if (o.cross_term) switch (o.kind) {
                .settle => self.cross_term_settles += 1,
                .expire => self.cross_term_expires += 1,
                else => {},
            };
        }

        pub fn add(self: *Counts, other: Counts) void {
            for (&self.applied, other.applied) |*a, b| a.* += b;
            for (&self.refused, other.refused) |*a, b| a.* += b;
            self.items_applied += other.items_applied;
            self.items_refused += other.items_refused;
            self.cross_term_settles += other.cross_term_settles;
            self.cross_term_expires += other.cross_term_expires;
        }
    };

    pub fn init(allocator: Allocator) Queues {
        var q: Queues = .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator), .rewrite = allocator.alloc(u8, @max(qe.Release.max_size, qe.Settle.max_size, qe.Expire.max_size, qe.Deliver.max_size)) catch @panic("sim OOM") };
        q.store.strict_keys = true;
        return q;
    }

    pub fn deinit(self: *Queues) void {
        self.store.allocator.free(self.rewrite);
        self.store.deinit();
        self.scratch.deinit();
    }

    pub fn reset(self: *Queues) void {
        self.store.clear();
        std.debug.assert(self.store.strict_keys);
    }

    pub fn owns(entry_type: u8) bool {
        for (qm.machine.entry_types) |t| if (@backingInt(t) == entry_type) return true;
        return false;
    }

    pub fn read(self: *Queues) view_mod.ReadView {
        return .{ .store = &self.store };
    }

    pub fn msg(self: *Queues, ns: NamespaceId, hash: [8]u8, seq: u64) ?Msg {
        var b: [4 + qe.ID_LEN]u8 = undefined;
        const id = qm.idOf(hash, seq);
        const got = self.read().get(qm.Msgs, qm.storeKey(&b, ns, &id)) catch return null;
        return if (got) |m| m.v else null;
    }

    /// The bytes every queue holds (`qm.heldBy`).
    pub fn shardHeld(self: *Queues) u64 {
        const bytes = self.store.get(qm.Helds.ID, qm.HELD_KEY) orelse return 0;
        return (qm.Helds.Value.decode(bytes) catch return 0).v.bytes;
    }

    pub fn meta(self: *Queues, ns: NamespaceId, hash: [8]u8) ?qm.Meta.Value {
        var b: [12]u8 = undefined;
        const got = self.read().get(qm.Metas, qm.storeKey(&b, ns, &hash)) catch return null;
        return if (got) |m| m.v else null;
    }

    fn kindOf(t: u8) Kind {
        return switch (@as(entry_mod.EntryType, @fromBackingInt(t))) {
            .queue_enqueue => .enqueue,
            .queue_deliver => .deliver,
            .queue_settle => .settle,
            .queue_release => .release,
            .queue_extend => .extend,
            .queue_expire => .expire,
            else => .purge,
        };
    }

    /// A queue's messages as they stood, by seq, copied out of the store.
    const Snapshot = struct {
        ns: NamespaceId,
        hash: [8]u8,
        msgs: std.AutoArrayHashMapUnmanaged(u64, Msg) = .empty,
        /// Each message's payload length: a deliver's byte budget counts it.
        sizes: std.AutoArrayHashMapUnmanaged(u64, usize) = .empty,
        /// Each message's error text length: what it holds counts it.
        errors: std.AutoArrayHashMapUnmanaged(u64, usize) = .empty,
        dead_lettered: u64 = 0,
        held: u64 = 0,
        held_bytes: u64 = 0,
        shard_held: u64 = 0,
    };

    fn snapshot(self: *Queues, a: Allocator, ns: NamespaceId, hash: [8]u8) Allocator.Error!Snapshot {
        var s: Snapshot = .{ .ns = ns, .hash = hash, .shard_held = self.shardHeld() };
        if (self.meta(ns, hash)) |m| {
            s.dead_lettered = m.dead_lettered;
            s.held = m.ready + m.leased + m.dead;
            s.held_bytes = m.held_bytes;
        }
        var pb: [12]u8 = undefined;
        var it = self.read().range(qm.Msgs, qm.storeKey(&pb, ns, &hash));
        while (it.next() catch null) |item| {
            var v = item.value.v;
            try s.sizes.put(a, qm.seqOf(item.key[4..]), v.payload.len);
            try s.errors.put(a, qm.seqOf(item.key[4..]), v.@"error".len);
            v.holder = try a.dupe(u8, v.holder);
            v.payload = "";
            v.queue = try a.dupe(u8, v.queue);
            v.@"error" = "";
            try s.msgs.put(a, qm.seqOf(item.key[4..]), v);
        }
        return s;
    }

    fn noteQueue(self: *Queues, a: Allocator, snaps: *std.ArrayListUnmanaged(Snapshot), ns: NamespaceId, hash: [8]u8) Allocator.Error!void {
        for (snaps.items) |s| if (s.ns == ns and std.mem.eql(u8, &s.hash, &hash)) return;
        try snaps.append(a, try self.snapshot(a, ns, hash));
    }

    /// Apply `e` as the shard does, then check it. A refusal must leave
    /// every family as it was.
    pub fn apply(self: *Queues, e: *const entry_mod.Entry, r: Replica) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        var diag: schema.Diagnostic = .{};
        const kind = kindOf(e.header.entry_type);
        var arena = std.heap.ArenaAllocator.init(self.store.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var snaps: std.ArrayListUnmanaged(Snapshot) = .empty;
        switch (kind) {
            .expire => {
                const p = qe.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |item| if (item.id.len == qe.ID_LEN) try self.noteQueue(a, &snaps, .of(item.ns), item.id[0..8].*);
            },
            inline else => |k| {
                const P = switch (k) {
                    .enqueue => qe.Enqueue,
                    .deliver => qe.Deliver,
                    .settle => qe.Settle,
                    .release => qe.Release,
                    .extend => qe.Extend,
                    .purge => qe.Purge,
                    else => unreachable,
                };
                const p = P.decodeAll(e.payload, &diag) catch return error.Halt;
                try self.noteQueue(a, &snaps, .of(p.ns), qm.hashOf(p.queue));
            },
        }

        const applied = try self.rewritten(e, r, snaps.items);
        var out: persistence.Result = .{};
        out.reset(&applied);
        const before = self.commits();
        try machine_mod.applyEntry(&qm.machine, &self.store, &self.scratch, &applied, &out);
        if (out.refused() != null) {
            if (!std.mem.eql(u64, &before, &self.commits())) return error.RefusalChangedState;
            // Past its caps, as the entry was proposed, an enqueue is
            // refused. The machine refuses nothing else the workload sends:
            // never a name whose hash slot holds another.
            if (kind == .enqueue) {
                const p = qe.Enqueue.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = snaps.items[0];
                if (over(p, s)) return .{ .kind = kind, .refused = true };
                return .{ .kind = kind, .refused = true, .broke = "an enqueue within its caps was refused" };
            }
            return .{ .kind = kind, .refused = true, .broke = "the queue machine refused a workload entry" };
        }
        if (r.mutant == .queue_settle_keeps_bytes and kind == .settle) try self.keepSettledBytes(snaps.items[0]);
        var effect: Effect = .{ .kind = kind, .refused = false };
        effect.broke = self.check(e, snaps.items, &effect) catch "the queue check ran out of memory";
        if (effect.broke == null) for (snaps.items) |s| if (self.ruleOfQueue(s.ns, s.hash)) |why| {
            effect.broke = why;
            break;
        };
        if (effect.broke == null) effect.broke = self.ruleOfShard();
        return effect;
    }

    /// Whether `p` would take its queue or its shard past the caps it
    /// carries, from where they stood.
    fn over(p: qe.Enqueue.Value, s: Snapshot) bool {
        const size = qm.heldBy(p.payload.len, 0);
        return s.held + 1 > p.max_messages or s.held_bytes + size > p.max_bytes or s.shard_held + size > p.shard_held_bytes;
    }

    fn holds(m: Msg, holder: []const u8, deliveries: u32, token: ae.Token.Value) bool {
        return m.status == .leased and m.deliveries == deliveries and m.token.term == token.term and m.token.index == token.index and std.mem.eql(u8, m.holder, holder);
    }

    fn after(stamp_ns: u64, millis: u64) u64 {
        return stamp_ns +| (millis *| std.time.ns_per_ms);
    }

    /// Whether a message's state moved: what a report that doesn't hold it
    /// must leave alone.
    fn moved(pre: Msg, post: Msg) bool {
        return pre.status != post.status or pre.priority != post.priority or pre.deliveries != post.deliveries or pre.failures != post.failures or pre.expiry_ns != post.expiry_ns or pre.due_ns != post.due_ns or pre.token.index != post.token.index or pre.token.term != post.token.term or pre.ended != post.ended or !std.mem.eql(u8, pre.holder, post.holder);
    }

    /// The rules an applied entry must keep, by the entry as it was
    /// proposed and its queue either side of it.
    fn check(self: *Queues, e: *const entry_mod.Entry, snaps: []const Snapshot, effect: *Effect) Allocator.Error!?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const stamp = e.header.timestamp_ns;
        const s = snaps[0];
        switch (effect.kind) {
            .enqueue => {
                const p = qe.Enqueue.decodeAll(e.payload, &diag) catch unreachable;
                const m = self.meta(s.ns, s.hash) orelse return "an enqueue left its queue with no meta";
                const v = self.msg(s.ns, s.hash, m.next_seq - 1) orelse return "an enqueue added no message at its seq";
                if (v.status != .ready or v.priority != p.priority or v.due_ns != after(stamp, p.delay_ms) or v.deliveries != 0 or v.failures != 0)
                    return "an enqueued message isn't ready at its delay with its priority";
                if (s.msgs.contains(m.next_seq - 1)) return "an enqueue reused a seq";
                if (over(p, s)) return "an enqueue past its caps applied";
                if (m.ready + m.leased + m.dead > p.max_messages or m.held_bytes > p.max_bytes or self.shardHeld() > p.shard_held_bytes)
                    return "an enqueue left its queue or its shard past the caps it carried";
            },
            .deliver => return self.checkDeliver(e, s, effect),
            .settle, .release, .extend => return self.checkReport(e, s, effect),
            .expire => {
                const p = qe.Expire.decodeAll(e.payload, &diag) catch unreachable;
                // First, any lease ended early: what an expire must never do.
                for (snaps) |sn| {
                    var it = sn.msgs.iterator();
                    while (it.next()) |kv| {
                        const pre = kv.value_ptr.*;
                        const post = self.msg(sn.ns, sn.hash, kv.key_ptr.*) orelse continue;
                        if (pre.status == .leased and post.status != .leased and pre.expiry_ns > stamp) return "an expire ended a lease before its deadline";
                    }
                }
                for (snaps) |sn| {
                    var died: u64 = 0;
                    var it = sn.msgs.iterator();
                    while (it.next()) |kv| {
                        const pre = kv.value_ptr.*;
                        const seq = kv.key_ptr.*;
                        const due_item = namedDue(p.items, sn, seq, pre, stamp);
                        const post = self.msg(sn.ns, sn.hash, seq) orelse return "an expire removed a message";
                        if (pre.status == .leased and post.status != .leased) {
                            if (!due_item) return "an expire ended a lease none of its items named the deadline of";
                            if (post.failures != pre.failures + 1) return "a message's failures don't count the lease that expired";
                            if (post.failures >= qe.MAX_ATTEMPTS) {
                                if (post.status != .dead) return "a message whose attempts are spent isn't dead";
                                died += 1;
                            } else if (post.status != .ready or post.due_ns != stamp) return "an expired message isn't ready again at once";
                            effect.applied += 1;
                            effect.cross_term = effect.cross_term or pre.token.term < e.header.term;
                        } else {
                            // A live lease it named, past due, must end.
                            if (due_item) return "an expire left a live lease it named past its deadline";
                            if (moved(pre, post)) return "an expire changed a message without ending its lease";
                        }
                    }
                    if (self.meta(sn.ns, sn.hash)) |m| if (m.dead_lettered != sn.dead_lettered + died) return "a dead message isn't counted dead once";
                }
            },
            .purge => {
                var pb: [12]u8 = undefined;
                var it = self.read().range(qm.Msgs, qm.storeKey(&pb, s.ns, &s.hash));
                if ((it.next() catch null) != null) return "a purge left a message in its queue";
            },
        }
        // Every message the entry didn't end: counters never go down.
        for (snaps) |sn| {
            var it = sn.msgs.iterator();
            while (it.next()) |kv| {
                const post = self.msg(sn.ns, sn.hash, kv.key_ptr.*) orelse continue;
                if (post.deliveries < kv.value_ptr.deliveries or post.failures < kv.value_ptr.failures) return "a message's deliveries or failures went down";
            }
        }
        return null;
    }

    /// Whether an expire's items name `seq`'s live lease, past due at
    /// `stamp`: one the expire must end.
    fn namedDue(items: anytype, sn: Snapshot, seq: u64, pre: Msg, stamp: u64) bool {
        if (pre.status != .leased or pre.expiry_ns > stamp) return false;
        var it = items.iterator();
        while (it.next() catch null) |item| {
            if (item.ns == sn.ns.int() and item.id.len == qe.ID_LEN and std.mem.eql(u8, item.id[0..8], &sn.hash) and qm.seqOf(item.id) == seq and item.expiry_ns == pre.expiry_ns) return true;
        }
        return false;
    }

    const Due = struct { seq: u64, priority: u32, due_ns: u64 };

    fn dueLess(_: void, x: Due, y: Due) bool {
        if (x.priority != y.priority) return x.priority < y.priority;
        if (x.due_ns != y.due_ns) return x.due_ns < y.due_ns;
        return x.seq < y.seq;
    }

    fn checkDeliver(self: *Queues, e: *const entry_mod.Entry, s: Snapshot, effect: *Effect) Allocator.Error!?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const p = qe.Deliver.decodeAll(e.payload, &diag) catch unreachable;
        const stamp = e.header.timestamp_ns;
        const a = self.scratch.allocator();
        // The due ready messages before it, in the order a deliver takes them.
        var due: std.ArrayListUnmanaged(Due) = .empty;
        var it = s.msgs.iterator();
        while (it.next()) |kv| {
            const v = kv.value_ptr.*;
            if (v.status == .ready and v.due_ns <= stamp) try due.append(a, .{ .seq = kv.key_ptr.*, .priority = v.priority, .due_ns = v.due_ns });
        }
        std.mem.sort(Due, due.items, {}, dueLess);
        // What this delivery leased.
        var taken: u32 = 0;
        // A change to a message it didn't lease, said after the order: what
        // it took is the first thing to judge.
        var changed_other = false;
        it = s.msgs.iterator();
        while (it.next()) |kv| {
            const pre = kv.value_ptr.*;
            const post = self.msg(s.ns, s.hash, kv.key_ptr.*) orelse return "a deliver removed a message";
            const ours = post.status == .leased and post.token.term == e.header.term and post.token.index == e.header.index;
            if (!ours) {
                if (moved(pre, post)) changed_other = true;
                continue;
            }
            taken += 1;
            if (pre.status != .ready) return "a deliver leased a message that wasn't ready";
            if (pre.due_ns > stamp) return "a message was delivered before it was due";
            if (!std.mem.eql(u8, post.holder, p.holder) or post.deliveries != pre.deliveries + 1) return "a deliver leased a message other than to the delivery it names";
            if (post.lease_ms != p.lease_ms or post.delivered_ns != stamp or post.expiry_ns != after(stamp, p.lease_ms)) return "a deliver's lease doesn't run from its stamp for its lease";
        }
        if (taken > p.count) return "a deliver leased more messages than it asked for";
        // When the picker could see every due message: it takes exactly the
        // first k in order, stopping at its count, at the end of the due
        // ones, or at the first whose payload would pass its byte budget
        // (the first is always taken).
        if (due.items.len <= qm.MAX_LOOKED_AT / 2) {
            var k: u32 = 0;
            var bytes: u64 = 0;
            for (due.items) |d| {
                if (k == p.count) break;
                const size = s.sizes.get(d.seq) orelse 0;
                if (k > 0 and bytes + size > p.max_bytes) break;
                bytes += size;
                k += 1;
            }
            for (due.items[0..@min(taken, due.items.len)]) |d| {
                const post = self.msg(s.ns, s.hash, d.seq) orelse return "a deliver removed a message";
                if (!(post.status == .leased and post.token.index == e.header.index and post.token.term == e.header.term)) return "a deliver didn't take the first due messages";
            }
            if (taken < k) return "a deliver took fewer messages than its count and budget allow";
            if (taken > k) return "a deliver took more messages than its byte budget allows";
        }
        if (changed_other) return "a deliver changed a message it didn't lease";
        effect.applied = taken;
        return null;
    }

    fn checkReport(self: *Queues, e: *const entry_mod.Entry, s: Snapshot, effect: *Effect) Allocator.Error!?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const stamp = e.header.timestamp_ns;
        const Common = struct { holder: []const u8, token: ae.Token.Value, items: qe.Items.Value };
        const c: Common = switch (effect.kind) {
            .settle => blk: {
                const p = qe.Settle.decodeAll(e.payload, &diag) catch unreachable;
                break :blk .{ .holder = p.holder, .token = p.token, .items = p.items };
            },
            .release => blk: {
                const p = qe.Release.decodeAll(e.payload, &diag) catch unreachable;
                break :blk .{ .holder = p.holder, .token = p.token, .items = p.items };
            },
            else => blk: {
                const p = qe.Extend.decodeAll(e.payload, &diag) catch unreachable;
                break :blk .{ .holder = p.holder, .token = p.token, .items = p.items };
            },
        };
        var died: u64 = 0;
        var named: std.AutoHashMapUnmanaged(u64, void) = .empty;
        var items = c.items.iterator();
        while (items.next() catch null) |item| {
            if ((try named.getOrPut(self.scratch.allocator(), item.seq)).found_existing) continue;
            const pre = s.msgs.get(item.seq) orelse {
                effect.refused_items += 1;
                continue;
            };
            const post_opt = self.msg(s.ns, s.hash, item.seq);
            if (!holds(pre, c.holder, item.deliveries, c.token)) {
                const post = post_opt orelse return "a report that names no live delivery removed a message";
                if (moved(pre, post)) return switch (effect.kind) {
                    .settle => "a settle that names no live delivery completed a message",
                    .release => "a release that names no live delivery ended one",
                    else => "an extend that names no live delivery moved a lease",
                };
                effect.refused_items += 1;
                continue;
            }
            effect.applied += 1;
            switch (effect.kind) {
                .settle => {
                    if (post_opt != null) return "a settle naming the live delivery didn't remove the message";
                    effect.cross_term = effect.cross_term or pre.token.term < e.header.term;
                },
                .release => {
                    const p = qe.Release.decodeAll(e.payload, &diag) catch unreachable;
                    const post = post_opt orelse return "a fail removed its message";
                    if (post.failures != pre.failures + 1) return "a message's failures don't count the deliveries that failed";
                    if (p.dead or post.failures >= qe.MAX_ATTEMPTS) {
                        if (post.status != .dead) return "a message failed to dead, or past its attempts, isn't dead";
                        died += 1;
                    } else if (post.status != .ready or post.due_ns != after(stamp, p.delay_ms)) return "a failed message isn't ready again after its delay";
                },
                else => {
                    const p = qe.Extend.decodeAll(e.payload, &diag) catch unreachable;
                    const post = post_opt orelse return "an extend removed a message";
                    if (p.lease_ms != pre.lease_ms or p.held_since_ns != pre.delivered_ns) {
                        if (moved(pre, post)) return "an extend naming another lease moved one";
                        continue;
                    }
                    const lease_ns = pre.lease_ms *| std.time.ns_per_ms;
                    const to = @min(after(stamp, p.lease_ms), after(pre.delivered_ns, ae.MAX_HOLD_MS));
                    const want = if (pre.expiry_ns -| stamp > lease_ns / 2 or to <= pre.expiry_ns) pre.expiry_ns else to;
                    if (post.expiry_ns != want) return "an extend naming the live delivery didn't move its lease as it should";
                },
            }
        }
        if (effect.kind == .release) {
            const m = self.meta(s.ns, s.hash) orelse return "a release left its queue with no meta";
            if (m.dead_lettered != s.dead_lettered + died) return "a dead message isn't counted dead once";
        }
        // Every message it didn't name is as it was.
        var rest = s.msgs.iterator();
        while (rest.next()) |kv| {
            if (named.contains(kv.key_ptr.*)) continue;
            const post = self.msg(s.ns, s.hash, kv.key_ptr.*) orelse return "a report changed a message it didn't name";
            if (moved(kv.value_ptr.*, post)) return "a report changed a message it didn't name";
        }
        return null;
    }

    /// What a queue must be after any entry: each message has the key its
    /// status says, spent ones are dead, and its counts are what it holds.
    fn ruleOfQueue(self: *Queues, ns: NamespaceId, hash: [8]u8) ?[]const u8 {
        var counts: [3]u64 = .{ 0, 0, 0 };
        var bytes: u64 = 0;
        var pb: [12]u8 = undefined;
        var it = self.read().range(qm.Msgs, qm.storeKey(&pb, ns, &hash));
        while (it.next() catch null) |item| {
            const v = item.value.v;
            const id = item.key[4..];
            counts[@backingInt(v.status)] += 1;
            bytes += qm.heldBy(v.payload.len, v.@"error".len);
            if (v.failures >= qe.MAX_ATTEMPTS and v.status != .dead) return "a message that spent its attempts isn't dead";
            var b: [64]u8 = undefined;
            var kb: [72]u8 = undefined;
            switch (v.status) {
                .ready => if (self.store.get(qm.Ready.ID, qm.storeKey(&kb, ns, qm.readyKeyOf(&b, v, v.due_ns, id))) == null) return "a ready message has no ready key at its due time",
                .leased => if (self.store.get(qm.Deadlines.ID, view_mod.key(&kb, "", qm.deadlineKey(&b, v.expiry_ns, ns, id)) catch return null) == null) return "a leased message has no deadline key at its expiry",
                .dead => {
                    const dk = qm.deadKey(hash, v.dead_ns, qm.seqOf(id));
                    if (self.store.get(qm.Dead.ID, qm.storeKey(&kb, ns, &dk)) == null) return "a dead message has no dead key";
                },
            }
        }
        const m = self.meta(ns, hash) orelse return if (counts[0] + counts[1] + counts[2] == 0) null else "a queue holds messages but no meta";
        if (m.ready != counts[0] or m.leased != counts[1] or m.dead != counts[2]) return "a queue's counts aren't what it holds";
        if (m.held_bytes != bytes) return "a queue's held bytes aren't what its messages hold";
        // And no ready or dead key is left over from a message that moved on.
        var rb: [9]u8 = undefined;
        var rk: [16]u8 = undefined;
        var ready_keys: u64 = 0;
        var ri = self.read().range(qm.Ready, qm.storeKey(&rk, ns, qm.readyPrefix(&rb, &hash)));
        while (ri.next() catch null) |_| ready_keys += 1;
        if (ready_keys != counts[0]) return "a queue has a ready key no ready message holds";
        var dead_keys: u64 = 0;
        var di = self.read().range(qm.Dead, qm.storeKey(&pb, ns, &hash));
        while (di.next() catch null) |_| dead_keys += 1;
        if (dead_keys != counts[2]) return "a queue has a dead key no dead message holds";
        return null;
    }

    /// The shard's held bytes are every queue's.
    fn ruleOfShard(self: *Queues) ?[]const u8 {
        var sum: u64 = 0;
        var it = self.read().range(qm.Metas, "");
        while (it.next() catch null) |item| sum += item.value.v.held_bytes;
        if (sum != self.shardHeld()) return "the shard's held bytes aren't its queues' held bytes";
        return null;
    }

    /// The entry a mutant applies in place of `e`; `e` itself otherwise. A
    /// mutant that moves state first does it here too.
    fn rewritten(self: *Queues, e: *const entry_mod.Entry, r: Replica, snaps: []const Snapshot) persistence.ApplyError!entry_mod.Entry {
        var out = e.*;
        var diag: schema.Diagnostic = .{};
        const kind = kindOf(e.header.entry_type);
        switch (r.mutant) {
            .queue_node_clock => out.header.timestamp_ns = r.node_wall_ns,
            .queue_expire_ignores_deadline => if (kind == .expire) {
                // As late as its latest named live lease: one not yet due
                // then ends.
                const p = qe.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |item| {
                    if (item.id.len != qe.ID_LEN) continue;
                    for (snaps) |sn| if (sn.ns.int() == item.ns and std.mem.eql(u8, &sn.hash, item.id[0..8])) if (sn.msgs.get(qm.seqOf(item.id))) |v| {
                        if (v.status == .leased and v.expiry_ns == item.expiry_ns) out.header.timestamp_ns = @max(out.header.timestamp_ns, item.expiry_ns);
                    };
                }
            },
            .queue_expire_first_only => if (kind == .expire) {
                // The first live lease named ends; later live ones are
                // dropped from the entry, stale keys kept.
                const p = qe.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
                var w: schema.Writer = .{ .buf = try self.scratch.allocator().alloc(u8, qe.Expire.max_size) };
                var n: u32 = 0;
                var lives: u32 = 0;
                var it = p.items.iterator();
                while (it.next() catch return error.Halt) |item| {
                    var live = false;
                    if (item.id.len == qe.ID_LEN) for (snaps) |sn| if (sn.ns.int() == item.ns and std.mem.eql(u8, &sn.hash, item.id[0..8])) if (sn.msgs.get(qm.seqOf(item.id))) |v| {
                        live = v.status == .leased and v.expiry_ns == item.expiry_ns and item.expiry_ns <= e.header.timestamp_ns;
                    };
                    if (live) {
                        lives += 1;
                        if (lives > 1) continue;
                    }
                    qe.ExpireItem.encode(&w, item) catch return error.Halt;
                    n += 1;
                }
                out.payload = qe.Expire.encodeInto(self.rewrite, .{ .items = .{ .bytes = w.buf[0..w.pos], .count = n } }) catch return error.Halt;
            },
            .queue_report_changes_unnamed => if (kind == .settle) {
                // The settle also moves a ready message it doesn't name.
                const p = qe.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = snaps[0];
                var it = s.msgs.iterator();
                outer: while (it.next()) |kv| {
                    if (kv.value_ptr.status != .ready) continue;
                    var items = p.items.iterator();
                    while (items.next() catch null) |item| if (item.seq == kv.key_ptr.*) continue :outer;
                    try self.demote(s.ns, s.hash, kv.key_ptr.*);
                    break;
                }
            },
            .queue_cap_tightened => if (kind == .enqueue) {
                // Caps one short of the entry's: an enqueue at the edge is
                // refused within them.
                var p = qe.Enqueue.decodeAll(e.payload, &diag) catch return error.Halt;
                p.max_messages -|= 1;
                p.max_bytes -|= 1;
                p.shard_held_bytes -|= 1;
                out.payload = qe.Enqueue.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .queue_cap_unchecked => if (kind == .enqueue) {
                // Caps so high nothing is ever refused.
                var p = qe.Enqueue.decodeAll(e.payload, &diag) catch return error.Halt;
                p.max_messages = std.math.maxInt(u32);
                p.max_bytes = std.math.maxInt(u64);
                p.shard_held_bytes = std.math.maxInt(u64);
                out.payload = qe.Enqueue.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .queue_deliver_short => if (kind == .deliver) {
                var p = qe.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
                p.count = 1;
                out.payload = qe.Deliver.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .queue_settle_ignores_token => if (kind == .settle) {
                // Every item named as its message's live delivery.
                var p = qe.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
                const s = snaps[0];
                const a = self.scratch.allocator();
                var w: schema.Writer = .{ .buf = try a.alloc(u8, qe.Items.max_size) };
                var items = p.items.iterator();
                while (items.next() catch return error.Halt) |item| {
                    var moved_item = item;
                    if (s.msgs.get(item.seq)) |v| if (v.status == .leased) {
                        p.holder = try a.dupe(u8, v.holder);
                        p.token = v.token;
                        p.expiry_ns = v.expiry_ns;
                        moved_item.deliveries = v.deliveries;
                    };
                    qe.Item.encode(&w, moved_item) catch return error.Halt;
                }
                p.items = .{ .bytes = w.buf[0..w.pos], .count = p.items.count };
                out.payload = qe.Settle.encodeInto(self.rewrite, p) catch return error.Halt;
            },
            .queue_release_removes => if (kind == .release) {
                const p = qe.Release.decodeAll(e.payload, &diag) catch return error.Halt;
                out.header.entry_type = @backingInt(entry_mod.EntryType.queue_settle);
                out.payload = qe.Settle.encodeInto(self.rewrite, .{ .ns = p.ns, .queue = p.queue, .holder = p.holder, .token = p.token, .expiry_ns = p.expiry_ns, .items = p.items }) catch return error.Halt;
            },
            .queue_deliver_wrong_order => if (kind == .deliver) {
                // The first due message moves to the back of the queue's
                // order, so the deliver takes the next in its place.
                const s = snaps[0];
                var best: ?Due = null;
                var it = s.msgs.iterator();
                while (it.next()) |kv| {
                    const v = kv.value_ptr.*;
                    if (v.status != .ready or v.due_ns > e.header.timestamp_ns) continue;
                    const d: Due = .{ .seq = kv.key_ptr.*, .priority = v.priority, .due_ns = v.due_ns };
                    if (best == null or dueLess({}, d, best.?)) best = d;
                }
                if (best) |b| try self.demote(s.ns, s.hash, b.seq);
            },
            else => {},
        }
        return out;
    }

    /// `queue_settle_keeps_bytes`: the messages a settle removed still
    /// count in their queue's held bytes.
    fn keepSettledBytes(self: *Queues, s: Snapshot) persistence.ApplyError!void {
        var kept: u64 = 0;
        var it = s.msgs.iterator();
        while (it.next()) |kv| if (self.msg(s.ns, s.hash, kv.key_ptr.*) == null) {
            kept += qm.heldBy(s.sizes.get(kv.key_ptr.*) orelse 0, s.errors.get(kv.key_ptr.*) orelse 0);
        };
        if (kept == 0) return;
        var mb: [12]u8 = undefined;
        const node = self.store.find(qm.Metas.ID, qm.storeKey(&mb, s.ns, &s.hash)) orelse return;
        var m = (qm.Metas.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
        m.held_bytes += kept;
        const bytes = try self.store.allocator.alloc(u8, qm.Meta.encodedSize(m) catch return error.Halt);
        _ = qm.Meta.encodeInto(bytes, m) catch return error.Halt;
        self.store.replace(qm.Metas.ID, node, bytes);
    }

    /// Give a ready message the lowest priority, in place, key and record:
    /// `queue_deliver_wrong_order`'s state half.
    fn demote(self: *Queues, ns: NamespaceId, hash: [8]u8, seq: u64) persistence.ApplyError!void {
        const id = qm.idOf(hash, seq);
        var mb: [4 + qe.ID_LEN]u8 = undefined;
        const mk = qm.storeKey(&mb, ns, &id);
        const node = self.store.find(qm.Msgs.ID, mk) orelse return;
        var v = (qm.Msgs.Value.decode(store_mod.Record.of(node).value) catch return error.Halt).v;
        if (v.priority == std.math.maxInt(u32)) return;
        var b: [64]u8 = undefined;
        var kb: [72]u8 = undefined;
        const old = qm.storeKey(&kb, ns, qm.readyKeyOf(&b, v, v.due_ns, &id));
        const old_node = self.store.find(qm.Ready.ID, old) orelse return;
        v.priority = std.math.maxInt(u32);
        // The new key first: `v` points into the record `replace` frees.
        var nb: [64]u8 = undefined;
        var nkb: [72]u8 = undefined;
        const new = qm.storeKey(&nkb, ns, qm.readyKeyOf(&nb, v, v.due_ns, &id));
        const rec = try self.store.allocRecord(new);
        const bytes = try self.store.allocator.alloc(u8, qm.Msg.encodedSize(v) catch return error.Halt);
        _ = qm.Msg.encodeInto(bytes, v) catch return error.Halt;
        self.store.unlink(qm.Ready.ID, old_node);
        self.store.replace(qm.Msgs.ID, node, bytes);
        self.store.link(qm.Ready.ID, rec, try self.store.allocator.alloc(u8, 0));
    }

    fn commits(self: *const Queues) [FAMILIES.len]u64 {
        var out: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &out) |fam, *c| c.* = self.store.families[fam].commits;
        return out;
    }

    pub fn checksums(self: *Queues) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
    }
};
