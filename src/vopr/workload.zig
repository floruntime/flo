//! VOPR Workload — deterministic data synthesis + oracle
//!
//! Synthesizes the operations a client fleet would submit, with
//! *self-verifying* payloads: every byte is a pure function of
//! (scenario seed, op id), so the checker recomputes expected content
//! instead of storing it, and a corrupted or substituted payload is
//! always detectable.
//!
//! The oracle tracks each op through its lifecycle:
//!   pending (proposed to a leader) → acked (committed) | lost (legal:
//!   a conflicting term overwrote the slot before commit).
//! Only ACKED ops carry guarantees — the durability invariant is
//! "every acked op appears in canonical history exactly once, at the
//! (term, index) it was acked at".

const std = @import("std");
const PRNG = @import("stdx").PRNG;
const entry_mod = @import("../storage/ual/entry.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const schema = @import("../protocol/schema.zig");
const Scenario = @import("scenario.zig").Scenario;

const Allocator = std.mem.Allocator;
pub const EntryType = entry_mod.EntryType;

/// Op kinds the workload synthesizes — opaque bytes to Raft, but a
/// realistic mix of entry types rather than a uniform one. KV ops decode as
/// their entries do, since replicas apply them through the KV machine: a
/// put's value starts with the op id, and a delete is a batch that deletes
/// a slot's key and tombstones a key naming the op, so every op is still
/// attributable from its payload alone. The marker key is the simulator's
/// own bookkeeping, there only so an oracle can name the op; no client
/// writes a delete this way.
const OP_TYPES = [_]EntryType{ .kv_put, .stream_append, .queue_enqueue, .ts_write, .kv_batch };

/// KV ops write a few keys over and over, across two namespaces, so
/// overwrites, deletes and history all happen.
const KV_SLOTS = 32;
/// Where a delete's marker key lives: `del\0d` and the op id in hex.
const MARKER_PREFIX = "del\x00d";
/// The largest payload a scenario draws: the size of the KV encoder's
/// scratch.
pub const MAX_PAYLOAD = 4096;

pub const OpState = enum {
    /// Proposed to a leader; commit outcome not yet observed.
    pending,
    /// The proposing leader committed it at (term, index). Loss after
    /// this point is a safety violation.
    acked,
    /// A conflicting entry took its slot before commit — legal loss.
    lost,
};

pub const Op = struct {
    id: u64,
    entry_type: EntryType,
    payload_len: u32,
    /// Wyhash of the payload — recomputable from (seed, id) alone.
    payload_hash: u64,
    state: OpState,
    /// Node that accepted the proposal.
    proposer: u32,
    term: u64,
    index: u64,
};

pub const Workload = struct {
    allocator: Allocator,
    seed: u64,
    payload_min: u32,
    payload_max: u32,
    next_op_id: u64,
    /// Indexed by op id — ids are dense from 0.
    ops: std.ArrayListUnmanaged(Op),
    /// Scratch for payload generation, sized to payload_max.
    scratch: []u8,

    acked_count: u64,
    lost_count: u64,

    pub fn init(allocator: Allocator, scenario: *const Scenario) !Workload {
        return .{
            .allocator = allocator,
            .seed = scenario.seed,
            .payload_min = scenario.payload_min,
            .payload_max = scenario.payload_max,
            .next_op_id = 0,
            .ops = .empty,
            .scratch = try allocator.alloc(u8, scenario.payload_max),
            .acked_count = 0,
            .lost_count = 0,
        };
    }

    pub fn deinit(self: *Workload) void {
        self.ops.deinit(self.allocator);
        self.allocator.free(self.scratch);
    }

    pub const Synthesized = struct {
        entry_type: EntryType,
        payload: []u8,
        hash: u64,
    };

    /// Deterministically derive the op with the given id into `buf`
    /// (must be ≥ payload_max). Pure function of (seed, op_id) — the
    /// checker calls this at verification time with no stored state.
    pub fn synthesize(seed: u64, payload_min: u32, payload_max: u32, op_id: u64, buf: []u8) Synthesized {
        std.debug.assert(payload_min >= 8); // op id occupies the first 8 bytes
        std.debug.assert(buf.len >= payload_max);
        // Mix the op id so consecutive ids don't produce correlated
        // streams from the xorshift state.
        var prng = PRNG.init(seed ^ (op_id *% 0x9E37_79B9_7F4A_7C15) ^ 0xF10F_10F1);
        const r = prng.random();
        std.debug.assert(payload_max <= MAX_PAYLOAD);
        var entry_type = OP_TYPES[r.uintLessThan(usize, OP_TYPES.len)];
        const len = r.intRangeAtMost(u32, payload_min, payload_max);
        const kv: ?[]u8 = switch (entry_type) {
            .kv_put => kvPut(buf[0..payload_max], op_id, len, r),
            .kv_batch => kvDelete(buf[0..payload_max], op_id, r),
            else => null,
        };
        const payload = kv orelse raw: {
            // A KV op too large for this scenario's payloads goes as raw bytes.
            if (entry_type == .kv_put or entry_type == .kv_batch) entry_type = .stream_append;
            const p = buf[0..len];
            // First 8 bytes carry the op id so a committed entry is
            // attributable from its payload alone.
            std.mem.writeInt(u64, p[0..8], op_id, .little);
            r.bytes(p[8..]);
            break :raw p;
        };
        const hash = std.hash.Wyhash.hash(0x5EED, payload);
        return .{ .entry_type = entry_type, .payload = payload, .hash = hash };
    }

    /// A put of a slot's key, `len` bytes long where the encoding allows:
    /// its value is the op id, then random bytes. A quarter carry a TTL.
    /// Null when it can't fit in `buf`.
    fn kvPut(buf: []u8, op_id: u64, len: u32, r: std.Random) ?[]u8 {
        var key_buf: [16]u8 = undefined;
        const key = slotKey(&key_buf, r.uintLessThan(u8, KV_SLOTS));
        const ttl_ms: ?u64 = if (r.uintLessThan(u8, 4) == 0) r.intRangeAtMost(u64, 1, 2000) else null;
        var value_buf: [MAX_PAYLOAD]u8 = undefined;
        // The value that brings the payload to `len`, but at least the op id.
        var value_len: usize = 8;
        const base = kv_entries.Put.encodedSize(.{ .key = key, .value = value_buf[0..8], .ttl_ms = ttl_ms }) catch return null;
        if (len > base) value_len += len - base;
        while (value_len > 8) : (value_len -= 1) {
            const size = kv_entries.Put.encodedSize(.{ .key = key, .value = value_buf[0..value_len], .ttl_ms = ttl_ms }) catch return null;
            if (size <= len) break;
        }
        const value = value_buf[0..value_len];
        std.mem.writeInt(u64, value[0..8], op_id, .little);
        r.bytes(value[8..]);
        const body: kv_entries.Put.Value = .{ .key = key, .value = value, .ttl_ms = ttl_ms };
        if ((kv_entries.Put.encodedSize(body) catch return null) > buf.len) return null;
        const out = kv_entries.Put.encodeInto(buf, body) catch return null;
        return buf[0..out.len];
    }

    /// A batch deleting a slot's key and tombstoning the op's marker key.
    fn kvDelete(buf: []u8, op_id: u64, r: std.Random) ?[]u8 {
        var key_buf: [16]u8 = undefined;
        var marker_buf: [MARKER_PREFIX.len + 16]u8 = undefined;
        const marker = std.fmt.bufPrint(&marker_buf, MARKER_PREFIX ++ "{x:0>16}", .{op_id}) catch unreachable;
        const ops = [_]kv_entries.BatchOp.Value{
            .{ .kind = .delete, .key = slotKey(&key_buf, r.uintLessThan(u8, KV_SLOTS)), .value = null, .ttl_ms = null, .delta = null },
            .{ .kind = .delete, .key = marker, .value = null, .ttl_ms = null, .delta = null },
        };
        var w: schema.Writer = .{ .buf = buf };
        kv_entries.BatchOps.encodeSlice(&w, &ops) catch return null;
        return buf[0..w.written().len];
    }

    /// A slot's key: every third slot in a namespace of its own.
    fn slotKey(buf: *[16]u8, slot: u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}k{d:0>2}", .{ if (slot % 3 == 0) "ns\x00" else "", slot }) catch unreachable;
    }

    /// Generate the next op. The payload points into the workload's
    /// scratch buffer — valid until the next `nextOp` or `expectedHash`
    /// call (both reuse `scratch`).
    pub fn nextOp(self: *Workload) !struct { id: u64, entry_type: EntryType, payload: []const u8 } {
        const id = self.next_op_id;
        self.next_op_id += 1;
        const gen = synthesize(self.seed, self.payload_min, self.payload_max, id, self.scratch);
        try self.ops.append(self.allocator, .{
            .id = id,
            .entry_type = gen.entry_type,
            .payload_len = @intCast(gen.payload.len),
            .payload_hash = gen.hash,
            .state = .pending,
            .proposer = 0,
            .term = 0,
            .index = 0,
        });
        return .{ .id = id, .entry_type = gen.entry_type, .payload = gen.payload };
    }

    /// Record where the accepting leader placed the op.
    pub fn recordProposal(self: *Workload, op_id: u64, proposer: u32, term: u64, index: u64) void {
        const op = &self.ops.items[op_id];
        op.proposer = proposer;
        op.term = term;
        op.index = index;
    }

    /// Ack: the proposing leader's commit_index reached the op's index
    /// while the entry there still carried the op's term. Idempotent.
    pub fn ack(self: *Workload, op_id: u64) void {
        const op = &self.ops.items[op_id];
        if (op.state == .pending) {
            op.state = .acked;
            self.acked_count += 1;
        }
    }

    /// Legal loss: a conflicting term overwrote the slot before commit.
    pub fn markLost(self: *Workload, op_id: u64) void {
        const op = &self.ops.items[op_id];
        if (op.state == .pending) {
            op.state = .lost;
            self.lost_count += 1;
        }
    }

    /// Recompute the expected payload hash for an op id.
    pub fn expectedHash(self: *Workload, op_id: u64) u64 {
        return synthesize(self.seed, self.payload_min, self.payload_max, op_id, self.scratch).hash;
    }

    /// Recover an op id from a committed payload: a KV put's value, a KV
    /// delete's marker key, otherwise the first 8 bytes. Null for payloads
    /// no workload op has (e.g. raft_noop).
    pub fn opIdFromPayload(entry_type: u8, payload: []const u8) ?u64 {
        var diag: schema.Diagnostic = .{};
        if (entry_type == @backingInt(EntryType.kv_put)) {
            const p = kv_entries.Put.decodeAll(payload, &diag) catch return null;
            if (p.value.len < 8) return null;
            return std.mem.readInt(u64, p.value[0..8], .little);
        }
        if (entry_type == @backingInt(EntryType.kv_batch)) {
            const b = kv_entries.Batch.decodeAll(payload, &diag) catch return null;
            var it = b.ops.iterator();
            while (it.next() catch return null) |op| {
                if (!std.mem.startsWith(u8, op.key, MARKER_PREFIX)) continue;
                return std.fmt.parseInt(u64, op.key[MARKER_PREFIX.len..], 16) catch null;
            }
            return null;
        }
        if (payload.len < 8) return null;
        return std.mem.readInt(u64, payload[0..8], .little);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "vopr workload: synthesis is deterministic and self-verifying" {
    var buf_a: [4096]u8 = undefined;
    var buf_b: [4096]u8 = undefined;
    for (0..50) |id| {
        const a = Workload.synthesize(99, 16, 2048, id, &buf_a);
        const b = Workload.synthesize(99, 16, 2048, id, &buf_b);
        try testing.expectEqual(a.hash, b.hash);
        try testing.expectEqual(a.entry_type, b.entry_type);
        try testing.expectEqualSlices(u8, a.payload, b.payload);
        try testing.expectEqual(@as(u64, id), Workload.opIdFromPayload(@backingInt(a.entry_type), a.payload).?);
    }
}

test "vopr workload: different seeds produce different payloads" {
    var buf_a: [4096]u8 = undefined;
    var buf_b: [4096]u8 = undefined;
    const a = Workload.synthesize(1, 16, 2048, 0, &buf_a);
    const b = Workload.synthesize(2, 16, 2048, 0, &buf_b);
    try testing.expect(a.hash != b.hash);
}

test "vopr workload: payload length respects bounds" {
    var buf: [4096]u8 = undefined;
    for (0..200) |id| {
        const gen = Workload.synthesize(7, 16, 100, id, &buf);
        try testing.expect(gen.payload.len <= 100);
        // A KV delete is as long as its encoding.
        if (gen.entry_type != .kv_batch) try testing.expect(gen.payload.len >= 16);
    }
}

test "vopr workload: KV ops decode as their entries, with every kind in the mix" {
    var buf: [4096]u8 = undefined;
    var puts: usize = 0;
    var deletes: usize = 0;
    var diag: schema.Diagnostic = .{};
    for (0..400) |id| {
        const gen = Workload.synthesize(5, 16, 2048, id, &buf);
        switch (gen.entry_type) {
            .kv_put => {
                _ = try kv_entries.Put.decodeAll(gen.payload, &diag);
                puts += 1;
            },
            .kv_batch => {
                _ = try kv_entries.Batch.decodeAll(gen.payload, &diag);
                deletes += 1;
            },
            else => {},
        }
    }
    try testing.expect(puts > 40 and deletes > 40);
}

test "vopr workload: op lifecycle and idempotent transitions" {
    const scenario = Scenario.calm(7);
    var w = try Workload.init(testing.allocator, &scenario);
    defer w.deinit();

    const op = try w.nextOp();
    try testing.expectEqual(@as(u64, 0), op.id);
    w.recordProposal(op.id, 1, 1, 2);
    w.ack(op.id);
    try testing.expectEqual(OpState.acked, w.ops.items[0].state);
    try testing.expectEqual(@as(u64, 1), w.acked_count);
    // Idempotent: a second ack or a late markLost must not double-count
    // or demote.
    w.ack(op.id);
    w.markLost(op.id);
    try testing.expectEqual(OpState.acked, w.ops.items[0].state);
    try testing.expectEqual(@as(u64, 1), w.acked_count);
    try testing.expectEqual(@as(u64, 0), w.lost_count);

    const op2 = try w.nextOp();
    w.recordProposal(op2.id, 2, 3, 9);
    w.markLost(op2.id);
    try testing.expectEqual(OpState.lost, w.ops.items[1].state);
    try testing.expectEqual(@as(u64, 1), w.lost_count);
}

test "vopr workload: expectedHash matches generated op" {
    const scenario = Scenario.calm(11);
    var w = try Workload.init(testing.allocator, &scenario);
    defer w.deinit();
    const op = try w.nextOp();
    const recorded = w.ops.items[op.id].payload_hash;
    try testing.expectEqual(recorded, w.expectedHash(op.id));
}
