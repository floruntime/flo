//! A family's history lane in the simulator: each chunk its group sends
//! itself is delivered to the history machine after the entry that sent
//! it, as a channel inbound, and checked as it lands against what landed
//! before it for that run: its first seq is the next the run hasn't
//! landed, its records are its seqs in order, and nothing lands after the
//! run's last chunk.

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

    const Inc = struct { next: u64 = 0, ended: bool = false };

    pub fn deinit(self: *Landed, a: std.mem.Allocator) void {
        self.runs.deinit(a);
    }

    pub fn clear(self: *Landed) void {
        self.runs.clearRetainingCapacity();
    }

    fn idOf(h: he.Header.Value) u64 {
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
        var k: u64 = 0;
        while (true) : (k += 1) {
            if (k + 1 >= STRIDE) return error.Halt;
            const next = channels.metaOf(rv, 0, .history).next_seq;
            const applied = if (channels.inOf(rv, 0, .history)) |i| i.applied else 0;
            if (applied + 1 >= next) return null;
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
            var res = persistence.Result.start(&ie);
            try machine_mod.applyEntry(&Lane.machine, store, scratch, &ie, &res);
            if (res.refused() != null) return error.Halt;
        }
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
        return null;
    }
};
