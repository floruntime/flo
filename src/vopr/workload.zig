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
/// writes a delete this way. A third of puts are JSON patches instead
/// (`kvPatch`), and a third of deletes also add to a counter (`kvDelete`).
const OP_TYPES = [_]EntryType{ .kv_put, .stream_append, .queue_enqueue, .ts_write, .kv_batch };

/// KV ops write a few keys over and over, across two namespaces, so
/// overwrites, deletes and history all happen.
const KV_SLOTS = 32;
/// Where a delete's marker key lives: `del\0d` and the op id in hex.
const MARKER_PREFIX = "del\x00d";
/// A patch's document opens with the op id, and a patch that deletes names
/// it in a field it deletes: `$.d` and the op id in hex.
const DOC_PREFIX = "{\"op\":";
const DEL_PATH_PREFIX = "$.d";
/// Patches write JSON keys of their own, so most land on a JSON value; one
/// in eight aims at a put's raw bytes instead.
const JSON_SLOTS = 4;
/// How far ahead a set at `$` looks for the deletes it writes fields for,
/// and at most how many.
const PATCH_LOOKAHEAD = 256;
const PATCH_DELETES_MAX = 32;
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
            .kv_put => if (r.uintLessThan(u8, 3) == 0) patch: {
                entry_type = .kv_json_patch;
                const shape = PatchShape.draw(r);
                break :patch kvPatch(buf[0..payload_max], .{ .seed = seed, .payload_min = payload_min, .payload_max = payload_max }, op_id, shape, len, r);
            } else kvPut(buf[0..payload_max], op_id, len, r),
            .kv_batch => kvDelete(buf[0..payload_max], op_id, r),
            else => null,
        };
        const payload = kv orelse raw: {
            // A KV op too large for this scenario's payloads goes as raw bytes.
            if (entry_type == .kv_put or entry_type == .kv_batch or entry_type == .kv_json_patch) entry_type = .stream_append;
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

    /// What a JSON patch does, and to which key: drawn first, so a set can
    /// tell from an op id alone what a later patch will be (`shapeOf`).
    const PatchShape = struct {
        /// At a put's raw bytes, which no patch but `$` can take.
        raw: bool,
        slot: u8,
        kind: enum { set_root, set_a, set_b, delete },

        fn draw(r: std.Random) PatchShape {
            const raw = r.uintLessThan(u8, 8) == 0;
            return .{
                .raw = raw,
                .slot = r.uintLessThan(u8, if (raw) KV_SLOTS else JSON_SLOTS),
                .kind = @fromBackingInt(@intCast(r.uintLessThan(u8, 4))),
            };
        }
    };

    const Draws = struct { seed: u64, payload_min: u32, payload_max: u32 };

    /// Op `op_id`'s patch shape, from the same draws `synthesize` makes;
    /// null when it isn't a JSON patch.
    fn shapeOf(d: Draws, op_id: u64) ?PatchShape {
        var prng = PRNG.init(d.seed ^ (op_id *% 0x9E37_79B9_7F4A_7C15) ^ 0xF10F_10F1);
        const r = prng.random();
        if (OP_TYPES[r.uintLessThan(usize, OP_TYPES.len)] != .kv_put) return null;
        _ = r.intRangeAtMost(u32, d.payload_min, d.payload_max);
        if (r.uintLessThan(u8, 3) != 0) return null;
        return PatchShape.draw(r);
    }

    /// A JSON patch.
    /// - A set at `$` writes a document with a field `d<id>` for each later
    ///   delete aimed at its key, so those deletes find something to delete.
    /// - A set at `$.a` or `$.b` merges with the key's value at apply.
    /// - A delete deletes its own `d<id>` field.
    /// Attributable either way: a set's document opens with the op id, and a
    /// delete's path names it. A patch the value can't take (raw bytes, no
    /// field, no key) is refused. Null when it can't fit in `buf`.
    fn kvPatch(buf: []u8, d: Draws, op_id: u64, shape: PatchShape, len: u32, r: std.Random) ?[]u8 {
        var key_buf: [16]u8 = undefined;
        const key = if (shape.raw) slotKey(&key_buf, shape.slot) else jsonKey(&key_buf, shape.slot);
        var path_buf: [DEL_PATH_PREFIX.len + 16]u8 = undefined;
        const path: []const u8 = switch (shape.kind) {
            .set_root => "$",
            .set_a => "$.a",
            .set_b => "$.b",
            .delete => std.fmt.bufPrint(&path_buf, DEL_PATH_PREFIX ++ "{x:0>16}", .{op_id}) catch unreachable,
        };
        var document: ?[]const u8 = null;
        var doc_buf: [MAX_PAYLOAD]u8 = undefined;
        if (shape.kind != .delete) {
            // {"op":<id>,"d<id>":0,…,"r":"<hex>"}, padded toward `len`.
            var w: std.Io.Writer = .fixed(doc_buf[0 .. doc_buf.len - 2]);
            w.writeAll(DOC_PREFIX) catch return null;
            w.print("{d}", .{op_id}) catch return null;
            if (shape.kind == .set_root and !shape.raw) {
                var written: usize = 0;
                var next = op_id + 1;
                // As many as fit the payload, with room for the rest of it.
                const field_len = ",\"d0000000000000000\":0".len;
                while (next <= op_id + PATCH_LOOKAHEAD and written < PATCH_DELETES_MAX) : (next += 1) {
                    const later = shapeOf(d, next) orelse continue;
                    if (later.raw or later.slot != shape.slot or later.kind != .delete) continue;
                    if (w.buffered().len + field_len + 32 > buf.len) break;
                    w.print(",\"d{x:0>16}\":0", .{next}) catch return null;
                    written += 1;
                }
            }
            w.writeAll(",\"r\":\"") catch return null;
            const used = w.buffered().len;
            const pad = @min(doc_buf.len - 2 - used, if (len > used + 32) len - used - 32 else 0);
            const hex = "0123456789abcdef";
            for (doc_buf[used..][0..pad]) |*c| c.* = hex[r.uintLessThan(u8, 16)];
            doc_buf[used + pad] = '"';
            doc_buf[used + pad + 1] = '}';
            document = doc_buf[0 .. used + pad + 2];
        }
        const body: kv_entries.JsonPatch.Value = .{ .key = key, .path = path, .document = document };
        if ((kv_entries.JsonPatch.encodedSize(body) catch return null) > buf.len) return null;
        const out = kv_entries.JsonPatch.encodeInto(buf, body) catch return null;
        return buf[0..out.len];
    }

    /// A batch deleting a slot's key and tombstoning the op's marker key.
    /// A third also add 1 to another slot's key between the two: on a put's
    /// raw bytes that refuses the whole batch after its delete is staged,
    /// the refusal that must drop staged writes.
    fn kvDelete(buf: []u8, op_id: u64, r: std.Random) ?[]u8 {
        var key_buf: [16]u8 = undefined;
        var counter_buf: [16]u8 = undefined;
        var marker_buf: [MARKER_PREFIX.len + 16]u8 = undefined;
        const marker = std.fmt.bufPrint(&marker_buf, MARKER_PREFIX ++ "{x:0>16}", .{op_id}) catch unreachable;
        const slot = slotKey(&key_buf, r.uintLessThan(u8, KV_SLOTS));
        const with_incr = r.uintLessThan(u8, 3) == 0;
        const counter = slotKey(&counter_buf, r.uintLessThan(u8, KV_SLOTS));
        const all = [_]kv_entries.BatchOp.Value{
            .{ .kind = .delete, .key = slot, .value = null, .ttl_ms = null, .delta = null },
            .{ .kind = .incr, .key = counter, .value = null, .ttl_ms = null, .delta = 1 },
            .{ .kind = .delete, .key = marker, .value = null, .ttl_ms = null, .delta = null },
        };
        const plain = [_]kv_entries.BatchOp.Value{ all[0], all[2] };
        var w: schema.Writer = .{ .buf = buf };
        kv_entries.BatchOps.encodeSlice(&w, if (with_incr) &all else &plain) catch return null;
        return buf[0..w.written().len];
    }

    /// A JSON slot's key: one only patches write.
    fn jsonKey(buf: *[16]u8, slot: u8) []const u8 {
        return std.fmt.bufPrint(buf, "j{d}", .{slot}) catch unreachable;
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
        if (entry_type == @backingInt(EntryType.kv_json_patch)) {
            const p = kv_entries.JsonPatch.decodeAll(payload, &diag) catch return null;
            if (p.document) |doc| {
                if (!std.mem.startsWith(u8, doc, DOC_PREFIX)) return null;
                const digits = doc[DOC_PREFIX.len..];
                const end = std.mem.indexOfScalar(u8, digits, ',') orelse return null;
                return std.fmt.parseInt(u64, digits[0..end], 10) catch null;
            }
            if (!std.mem.startsWith(u8, p.path, DEL_PATH_PREFIX)) return null;
            return std.fmt.parseInt(u64, p.path[DEL_PATH_PREFIX.len..], 16) catch null;
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
    var patches: usize = 0;
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
            .kv_json_patch => {
                _ = try kv_entries.JsonPatch.decodeAll(gen.payload, &diag);
                patches += 1;
            },
            else => {},
        }
    }
    try testing.expect(puts > 40 and deletes > 40 and patches > 20);
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
