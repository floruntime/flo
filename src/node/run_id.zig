//! Self-routing Run IDs
//!
//! Format: `{prefix}{hex(u64)}-{hex(shard)}`, e.g. `act-1a2b3c4d5e6f00-3`
//!
//! The packed u64 encodes:
//!   ```
//!   [timestamp_ms: 42 bits][partition_id: 14 bits][sequence: 8 bits]
//!   ```
//!
//! This makes every run ID self-routing: the server extracts the partition
//! directly from the ID bits, with no entity-name lookup required. The suffix
//! names the shard that minted it: a shard mints ids for partitions it does
//! not own (a workflow starting an action on another shard), so two shards'
//! generators share a partition's id space and only the suffix keeps them
//! apart.
//!
//! ## Constraints
//!
//! - Max partition ID: 16,383 (14 bits). Configure `partition_count ≤ 16384`.
//! - Sequence: 256 per millisecond per shard; past that the id's millisecond
//!   runs ahead of the clock rather than repeat an id.
//! - Timestamp range: ~139 years from custom epoch (2024-01-01 → ~2163).
//! - Max encoded length: prefix (4) + hex (16) + '-' + shard hex (2) = 23 bytes.

const std = @import("std");

/// Custom epoch: 2024-01-01T00:00:00Z in milliseconds.
const EPOCH_MS: u64 = 1_704_067_200_000;

/// Maximum partition ID encodable in 14 bits.
pub const MAX_PARTITION: u32 = 0x3FFF; // 16383

/// Maximum total length of a generated run ID.
pub const MAX_ID_LEN: usize = 23;

// ═══════════════════════════════════════════════════════════════════════════════
// Prefix
// ═══════════════════════════════════════════════════════════════════════════════

pub const Prefix = enum {
    workflow,
    action,
    job,
    savepoint,

    pub fn string(self: Prefix) []const u8 {
        return switch (self) {
            .workflow => "wfr-",
            .action => "act-",
            .job => "job-",
            .savepoint => "sp-",
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Generator
// ═══════════════════════════════════════════════════════════════════════════════

/// Per-shard run ID generator. Single-threaded — no atomics needed.
pub const Generator = struct {
    /// The shard this generator mints for, written into every id.
    shard: u16 = 0,
    last_ms: u64 = 0,
    sequence: u8 = 0,

    /// Generate a new run ID into `buf` (at least `MAX_ID_LEN` bytes).
    /// Never fails, and never repeats while the process runs: the
    /// millisecond it records is a logical clock that does not go back when
    /// the wall clock does, and moves on by one when a millisecond's 256 ids
    /// are spent.
    ///
    /// `partition_id` is the partition this entity belongs to (from router).
    /// The caller must ensure `partition_id <= MAX_PARTITION`.
    pub fn next(self: *Generator, prefix: Prefix, partition_id: u32, buf: []u8) []const u8 {
        std.debug.assert(buf.len >= MAX_ID_LEN);
        const now_ms = @max(currentMs(), self.last_ms);

        if (now_ms == self.last_ms) {
            if (self.sequence == 255) {
                self.last_ms += 1;
                self.sequence = 0;
            } else self.sequence += 1;
        } else {
            self.last_ms = now_ms;
            self.sequence = 0;
        }

        const ts_bits: u64 = self.last_ms & 0x3FF_FFFF_FFFF; // 42 bits
        const part_bits: u64 = @as(u64, partition_id & 0x3FFF); // 14 bits
        const seq_bits: u64 = @as(u64, self.sequence); // 8 bits

        const id_bits: u64 = (ts_bits << 22) | (part_bits << 8) | seq_bits;

        const pfx = prefix.string();
        @memcpy(buf[0..pfx.len], pfx);
        var len = pfx.len + hexEncode(id_bits, buf[pfx.len..]);
        buf[len] = '-';
        len += 1;
        len += hexEncode(self.shard, buf[len..]);
        return buf[0..len];
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Extraction
// ═══════════════════════════════════════════════════════════════════════════════

/// Extract the partition ID from a run ID string.
/// Returns null if the ID cannot be parsed.
pub fn extractPartition(run_id: []const u8) ?u32 {
    const id_bits = decodePayload(run_id) orelse return null;
    return @intCast((id_bits >> 8) & 0x3FFF);
}

/// Extract the creation timestamp (Unix ms) from a run ID string.
pub fn extractTimestamp(run_id: []const u8) ?u64 {
    const id_bits = decodePayload(run_id) orelse return null;
    return (id_bits >> 22) + EPOCH_MS;
}

/// Whether `run_id` has the form this generator mints for `prefix`
/// (`{prefix}{hex}-{hex}`). A client may not choose such an id: one the
/// server mints later could equal it.
pub fn looksGenerated(prefix: Prefix, run_id: []const u8) bool {
    const p = prefix.string();
    if (!std.mem.startsWith(u8, run_id, p)) return false;
    const rest = run_id[p.len..];
    const dash = std.mem.indexOfScalar(u8, rest, '-') orelse return false;
    return hexDecode(rest[0..dash]) != null and dash + 1 < rest.len and hexDecode(rest[dash + 1 ..]) != null;
}

/// Decode the hex payload from a prefixed run ID: what follows the first
/// dash, up to the shard suffix if there is one.
fn decodePayload(run_id: []const u8) ?u64 {
    const dash_pos = std.mem.indexOfScalar(u8, run_id, '-') orelse return null;
    if (dash_pos + 1 >= run_id.len) return null;
    const rest = run_id[dash_pos + 1 ..];
    const end = std.mem.indexOfScalar(u8, rest, '-') orelse rest.len;
    return hexDecode(rest[0..end]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pre-routing helper
// ═══════════════════════════════════════════════════════════════════════════════

/// Pre-route function for run-ID-based opcodes.
///
/// Extracts the partition directly from the packed ID bits in the key.
/// Returns null (broadcast/walk) if the ID cannot be decoded.
pub fn preRouteByRunId(req: @import("../protocol/proto.zig").Request) ?u64 {
    if (req.key.len == 0) return 0;
    const partition_id = extractPartition(req.key) orelse return null;
    // Return partition_id directly — Router.hashToPartition(partition_id)
    // yields partition_id when partition_id < partition_count.
    return @as(u64, partition_id);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hex Codec
// ═══════════════════════════════════════════════════════════════════════════════

const HEX_ALPHABET = "0123456789abcdef";

/// Encode a u64 as lowercase hex into `buf`, no leading zeros. Returns bytes written.
pub fn hexEncode(value: u64, buf: []u8) usize {
    if (value == 0) {
        buf[0] = '0';
        return 1;
    }
    // Count hex digits needed
    var v = value;
    var digits: usize = 0;
    while (v > 0) : (v >>= 4) {
        digits += 1;
    }
    // Write from right to left
    v = value;
    var i: usize = digits;
    while (i > 0) {
        i -= 1;
        buf[i] = HEX_ALPHABET[@intCast(v & 0xF)];
        v >>= 4;
    }
    return digits;
}

/// Decode a lowercase hex string to u64. Returns null on invalid input.
pub fn hexDecode(encoded: []const u8) ?u64 {
    if (encoded.len == 0 or encoded.len > 16) return null;
    var result: u64 = 0;
    for (encoded) |c| {
        const digit: u64 = switch (c) {
            '0'...'9' => c - '0',
            'a'...'f' => c - 'a' + 10,
            'A'...'F' => c - 'A' + 10, // accept uppercase on decode
            else => return null,
        };
        result = (result << 4) | digit;
    }
    return result;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Time
// ═══════════════════════════════════════════════════════════════════════════════

fn currentMs() u64 {
    const now: u64 = @bitCast(@import("stdx").time.milliTimestamp());
    if (now < EPOCH_MS) return 0;
    return now - EPOCH_MS;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "hex roundtrip" {
    const cases = [_]u64{ 0, 1, 15, 16, 255, 256, 1000, 999999, 0xDEADBEEF, std.math.maxInt(u64) };
    for (cases) |val| {
        var buf: [16]u8 = undefined;
        const len = hexEncode(val, &buf);
        const decoded = hexDecode(buf[0..len]).?;
        try std.testing.expectEqual(val, decoded);
    }
}

test "run ID encode/decode partition roundtrip" {
    var gen = Generator{};
    const partitions = [_]u32{ 0, 1, 42, 1000, 4095, 16383 };
    for (partitions) |pid| {
        var buf: [32]u8 = undefined;
        const id = gen.next(.workflow, pid, &buf);
        const extracted = extractPartition(id).?;
        try std.testing.expectEqual(pid, extracted);
    }
}

test "run IDs are unique" {
    var gen = Generator{};
    var ids: [10]u64 = undefined;
    for (&ids) |*slot| {
        var buf: [32]u8 = undefined;
        const id = gen.next(.workflow, 100, &buf);
        slot.* = decodePayload(id).?;
    }
    // All IDs should be distinct
    for (0..ids.len) |i| {
        for (i + 1..ids.len) |j| {
            try std.testing.expect(ids[i] != ids[j]);
        }
    }
}

test "prefix strings" {
    var gen = Generator{};
    var buf: [32]u8 = undefined;

    const wf = gen.next(.workflow, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, wf, "wfr-"));

    const act = gen.next(.action, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, act, "act-"));

    const job = gen.next(.job, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, job, "job-"));

    const sp = gen.next(.savepoint, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, sp, "sp-"));
}

test "extractPartition returns null for invalid IDs" {
    try std.testing.expectEqual(@as(?u32, null), extractPartition(""));
    try std.testing.expectEqual(@as(?u32, null), extractPartition("no-dash-here!"));
}

test "preRouteByRunId extracts partition" {
    const proto = @import("../protocol/proto.zig");
    var gen = Generator{};
    var buf: [32]u8 = undefined;
    const id = gen.next(.workflow, 777, &buf);

    const req = proto.Request{
        .header = std.mem.zeroes(proto.RequestHeader),
        .namespace = "default",
        .key = id,
        .value = "",
    };

    const hash = preRouteByRunId(req).?;
    try std.testing.expectEqual(@as(u64, 777), hash);
}

test "looksGenerated: the generator's own form, and nothing else" {
    var gen = Generator{};
    var buf: [MAX_ID_LEN]u8 = undefined;
    const minted = gen.next(.workflow, 7, &buf);
    try std.testing.expect(looksGenerated(.workflow, minted));
    try std.testing.expect(looksGenerated(.workflow, "wfr-1a2b-3"));
    // A client's own numbering never collides with the minted form.
    try std.testing.expect(!looksGenerated(.workflow, "wfr-2025"));
    try std.testing.expect(!looksGenerated(.workflow, "wfr-1a2b-"));
    try std.testing.expect(!looksGenerated(.workflow, "wfr-"));
    try std.testing.expect(!looksGenerated(.workflow, "wfr-order-17"));
    try std.testing.expect(!looksGenerated(.workflow, "order-123"));
    try std.testing.expect(!looksGenerated(.action, minted));
}

test "two shards minting for one partition in one millisecond never mint the same id" {
    var a = Generator{ .shard = 0 };
    var b = Generator{ .shard = 1 };
    var buf_a: [MAX_ID_LEN]u8 = undefined;
    var buf_b: [MAX_ID_LEN]u8 = undefined;
    // Same clock, same partition, same sequence: only the suffix differs.
    a.last_ms = currentMs() + 1000;
    b.last_ms = a.last_ms;
    const ia = a.next(.action, 7, &buf_a);
    const ib = b.next(.action, 7, &buf_b);
    try std.testing.expect(!std.mem.eql(u8, ia, ib));
    try std.testing.expectEqual(extractPartition(ia), extractPartition(ib));
}

test "a generator never repeats an id: not past 256 in a millisecond, not when the clock goes back" {
    var gen = Generator{ .shard = 2 };
    var seen = std.StringHashMap(void).init(std.testing.allocator);
    defer {
        var it = seen.keyIterator();
        while (it.next()) |k| std.testing.allocator.free(k.*);
        seen.deinit();
    }
    // Pinned ahead of the wall clock: every call lands in the same logical
    // millisecond until its sequence is spent, then the next.
    gen.last_ms = currentMs() + 60_000;
    var buf: [MAX_ID_LEN]u8 = undefined;
    var i: usize = 0;
    while (i < 600) : (i += 1) {
        const id = gen.next(.workflow, 5, &buf);
        const gop = try seen.getOrPut(try std.testing.allocator.dupe(u8, id));
        try std.testing.expect(!gop.found_existing);
    }
    try std.testing.expectEqual(@as(usize, 600), seen.count());
}
