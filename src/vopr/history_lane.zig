//! A family's history lane in the simulator: each chunk its group sends
//! itself is delivered to the history machine after the entry that sent
//! it, as a channel inbound, and checked as it lands against what landed
//! before it for that run: its first seq is the next the run hasn't
//! landed, its records are its seqs in order, and nothing lands after the
//! run's last chunk. The records each apply landed are kept until the
//! next, for its family to check against the entry that sent them.
//!
//! Once a deliver lands anything, the lane's taken messages are trimmed
//! after it, all of them or the older half, as an entry's index picks, so
//! the lane's per-namespace rows see trims, partial ones and none, and the
//! conservation rule (`historyTakenRule`) is checked across them. A
//! deliver that lands nothing trims nothing, so a refused entry still
//! leaves the store as it was.
//!
//! A deleted run's drop is lost, as a repair's skip loses it, as many times
//! as `dropLosses` says for its incarnation: none, once or twice for some,
//! as many as the machine's retries for others, so each family sees a drop land, land
//! after resends, and leave its run's rows orphaned.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const he = @import("../protocol/history_entries.zig");
const ce = @import("../protocol/channel_entries.zig");
const hp = @import("../stream/history_producer.zig");
const hm = @import("../stream/history_machine.zig");
const channels = @import("../contract/channels.zig");
const view_mod = @import("../contract/view.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");

const Lane = channels.Channels(&.{hm.machine});

/// An entry's inbounds take indexes of their own past its index: at most
/// this many per entry.
pub const STRIDE = 1 << 16;

/// What landed of each run incarnation's history: the seq it lands next,
/// and whether its last chunk did.
pub const Landed = struct {
    runs: std.AutoHashMapUnmanaged(u64, Inc) = .empty,
    /// The records of each chunk the last `deliver` landed, by run.
    fresh: std.ArrayListUnmanaged(Fresh) = .empty,

    pub const Fresh = struct { id: u64, records: []u8 };

    const Inc = struct { next: u64 = 0, ended: bool = false };

    pub fn deinit(self: *Landed, a: std.mem.Allocator) void {
        self.dropFresh(a);
        self.fresh.deinit(a);
        self.runs.deinit(a);
    }

    pub fn clear(self: *Landed, a: std.mem.Allocator) void {
        self.dropFresh(a);
        self.runs.clearRetainingCapacity();
    }

    fn dropFresh(self: *Landed, a: std.mem.Allocator) void {
        for (self.fresh.items) |f| a.free(f.records);
        self.fresh.clearRetainingCapacity();
    }

    /// The records the last `deliver` landed for one run incarnation, in
    /// the order they landed; each slice holds until the next `deliver`.
    pub fn freshOf(self: *const Landed, out: *std.ArrayListUnmanaged([]const u8), a: std.mem.Allocator, ns: u32, family: he.Family, owner: []const u8, run: []const u8, created_ns: u64) !void {
        const id = idOf(.{ .ns = ns, .family = @backingInt(family), .owner = owner, .run = run, .created_ns = created_ns, .first_seq = 0, .last_seq = 0, .final = false, .completed_ms = 0 });
        for (self.fresh.items) |f| if (f.id == id) try out.append(a, f.records);
    }

    pub fn idOf(h: he.Header.Value) u64 {
        var w = std.hash.Wyhash.init(0);
        w.update(std.mem.asBytes(&h.ns));
        w.update(&.{h.family});
        w.update(h.owner);
        w.update(&.{0});
        w.update(h.run);
        w.update(std.mem.asBytes(&h.created_ns));
        return w.final();
    }

    /// Every message the group sent itself on its history lane and hasn't
    /// applied, delivered after entry `e`, each checked as it lands: the
    /// rule a chunk broke, if one did.
    pub fn deliver(self: *Landed, store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, e: *const entry_mod.Entry) persistence.ApplyError!?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = store };
        // Not in `scratch`: each apply resets it.
        const a = store.allocator;
        self.dropFresh(a);
        var k: u64 = 0;
        while (true) : (k += 1) {
            if (k + 2 >= STRIDE) return error.Halt;
            const next = channels.metaOf(rv, 0, .history).next_seq;
            const applied = if (channels.inOf(rv, 0, .history)) |i| i.applied else 0;
            if (applied + 1 >= next) {
                if (k > 0) try trim(store, scratch, e, applied);
                return null;
            }
            const seq = applied + 1;
            var kb: [channels.OUT_KEY]u8 = undefined;
            const o = ((rv.get(channels.OutFam, channels.outKey(&kb, 0, .history, seq)) catch return error.Halt) orelse return error.Halt).v;
            if (o.entry_type == @backingInt(entry_mod.EntryType.history_chunk)) {
                if (try self.check(a, o.payload)) |why| return why;
            }
            const mb = try a.alloc(u8, o.payload.len + 64);
            defer a.free(mb);
            var w: schema.Writer = .{ .buf = mb };
            ce.Message.encode(&w, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = o.flags, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload }) catch return error.Halt;
            const ib = try a.alloc(u8, w.written().len + 64);
            defer a.free(ib);
            const inbound = ce.Inbound.encodeInto(ib, .{ .from = 0, .lane = @backingInt(channels.Lane.history), .first_seq = seq, .messages = .{ .bytes = w.written(), .count = 1 } }) catch return error.Halt;
            var h = std.mem.zeroes(entry_mod.Header);
            h.entry_type = @backingInt(entry_mod.EntryType.channel_inbound);
            h.index = e.header.index * STRIDE + k + 1;
            h.timestamp_ns = e.header.timestamp_ns;
            h.payload_len = @intCast(inbound.len);
            const ie: entry_mod.Entry = .{ .header = h, .payload = inbound };
            if (o.entry_type == @backingInt(entry_mod.EntryType.history_drop)) {
                var diag: schema.Diagnostic = .{};
                const d = he.Drop.decodeAll(o.payload, &diag) catch return "a history drop doesn't decode";
                if (d.first_seq < dropLosses(d)) store.inbound_skip = .all(h.index);
            }
            defer store.inbound_skip = null;
            var res = persistence.Result.start(&ie);
            try machine_mod.applyEntry(&Lane.machine, store, scratch, &ie, &res);
            if (res.refused() != null) return error.Halt;
        }
    }

    /// The lane's taken messages trimmed after entry `e`'s delivers: all,
    /// or the older half; on every third index none.
    fn trim(store: *store_mod.Store, scratch: *std.heap.ArenaAllocator, e: *const entry_mod.Entry, applied: u64) persistence.ApplyError!void {
        const acked = channels.metaOf(.{ .store = store }, 0, .history).acked;
        if (applied <= acked or e.header.index % 3 == 0) return;
        const span = applied - acked;
        const through = acked + @min(if (e.header.index % 3 == 1) span else (span + 1) / 2, ce.MAX_TRIM_SPAN);
        var tb: [64]u8 = undefined;
        const payload = ce.Trim.encodeInto(&tb, .{ .to = 0, .lane = @backingInt(channels.Lane.history), .first = acked + 1, .through = through }) catch return error.Halt;
        var h = std.mem.zeroes(entry_mod.Header);
        h.entry_type = @backingInt(entry_mod.EntryType.channel_trim);
        h.index = e.header.index * STRIDE + STRIDE - 1;
        h.timestamp_ns = e.header.timestamp_ns;
        h.payload_len = @intCast(payload.len);
        const te: entry_mod.Entry = .{ .header = h, .payload = payload };
        var res = persistence.Result.start(&te);
        try machine_mod.applyEntry(&Lane.machine, store, scratch, &te, &res);
        if (res.refused() != null) return error.Halt;
    }

    /// The ids of the run incarnations whose chunks the last `deliver`
    /// landed, as `idOf` names them.
    pub fn freshIds(self: *const Landed) []const Fresh {
        return self.fresh.items;
    }

    /// Chunk `payload` against what landed of its run before it.
    fn check(self: *Landed, a: std.mem.Allocator, payload: []const u8) persistence.ApplyError!?[]const u8 {
        var diag: schema.Diagnostic = .{};
        const c = he.Chunk.decodeAll(payload, &diag) catch return "a history chunk doesn't decode";
        const got = try self.runs.getOrPut(a, idOf(.{ .ns = c.ns, .family = c.family, .owner = c.owner, .run = c.run, .created_ns = c.created_ns, .first_seq = c.first_seq, .last_seq = c.last_seq, .final = c.final, .completed_ms = c.completed_ms }));
        if (!got.found_existing) got.value_ptr.* = .{};
        const inc = got.value_ptr;
        if (inc.ended) return "a run's history chunk landed after its last";
        if (c.first_seq != inc.next) return "a run's history chunk doesn't start at the next seq it hasn't landed";
        var r: schema.Reader = .{ .bytes = c.records, .diag = &diag };
        var seq = c.first_seq;
        while (r.pos < c.records.len) : (seq += 1) {
            const rec = hp.HistRec.decode(&r, "rec") catch return "a run's history chunk's records don't decode";
            if (rec.seq != seq) return "a run's history chunk's records aren't its seqs in order";
        }
        if (seq > c.first_seq and seq - 1 != c.last_seq) return "a run's history chunk's records aren't its seqs in order";
        inc.next = seq;
        inc.ended = c.final;
        try self.fresh.append(a, .{ .id = got.key_ptr.*, .records = try a.dupe(u8, c.records) });
        return null;
    }
};

/// How many times a run incarnation's drop is lost, by its incarnation, so
/// every replica loses the same ones: none, once, twice (each then resent
/// and landed), or `hm.MAX_DROP_RETRIES` times, past which its rows are
/// orphaned.
pub fn dropLosses(h: anytype) u64 {
    var w = std.hash.Wyhash.init(0xd0d0);
    w.update(h.owner);
    w.update(&.{0});
    w.update(h.run);
    w.update(std.mem.asBytes(&h.created_ns));
    return w.final() % (hm.MAX_DROP_RETRIES + 1);
}

/// Whether a run incarnation's drop is lost past its retries.
pub fn orphaned(owner: []const u8, run: []const u8, created_ns: u64) bool {
    return dropLosses(.{ .owner = owner, .run = run, .created_ns = created_ns }) >= hm.MAX_DROP_RETRIES;
}
