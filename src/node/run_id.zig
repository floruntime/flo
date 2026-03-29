//! Self-routing Run IDs
//!
//! Format: `{prefix}-{hex(u64)}`
//!
//! The packed u64 encodes:
//!   ```
//!   [timestamp_ms: 42 bits][partition_id: 14 bits][sequence: 8 bits]
//!   ```
//!
//! This makes every run ID self-routing: the server extracts the partition
//! directly from the ID bits, with no entity-name lookup required.
//!
//! ## Constraints
//!
//! - Max partition ID: 16,383 (14 bits). Configure `partition_count ≤ 16384`.
//! - Max sequence: 255 per millisecond per shard (single-threaded — plenty).
//! - Timestamp range: ~139 years from custom epoch (2024-01-01 → ~2163).
//! - Max encoded length: prefix (4) + hex (16) = 20 bytes.

const std = @import("std");

/// Custom epoch: 2024-01-01T00:00:00Z in milliseconds.
const EPOCH_MS: u64 = 1_704_067_200_000;

/// Maximum partition ID encodable in 14 bits.
pub const MAX_PARTITION: u32 = 0x3FFF; // 16383

/// Maximum total length of a generated run ID (prefix + hex).
pub const MAX_ID_LEN: usize = 20;

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
    last_ms: u64 = 0,
    sequence: u8 = 0,

    /// Generate a new run ID into `buf`. Returns the slice written.
    ///
    /// `partition_id` is the partition this entity belongs to (from router).
    /// The caller must ensure `partition_id <= MAX_PARTITION`.
    pub fn next(self: *Generator, prefix: Prefix, partition_id: u32, buf: []u8) error{SequenceExhausted}![]const u8 {
        const now_ms = currentMs();

        if (now_ms == self.last_ms) {
            if (self.sequence == 255) return error.SequenceExhausted;
            self.sequence += 1;
        } else {
            self.last_ms = now_ms;
            self.sequence = 0;
        }

        const ts_bits: u64 = now_ms & 0x3FF_FFFF_FFFF; // 42 bits
        const part_bits: u64 = @as(u64, partition_id & 0x3FFF); // 14 bits
        const seq_bits: u64 = @as(u64, self.sequence); // 8 bits

        const id_bits: u64 = (ts_bits << 22) | (part_bits << 8) | seq_bits;

        const pfx = prefix.string();
        @memcpy(buf[0..pfx.len], pfx);
        const enc_len = hexEncode(id_bits, buf[pfx.len..]);
        return buf[0 .. pfx.len + enc_len];
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

/// Decode the hex payload from a prefixed run ID.
fn decodePayload(run_id: []const u8) ?u64 {
    const dash_pos = std.mem.indexOfScalar(u8, run_id, '-') orelse return null;
    if (dash_pos + 1 >= run_id.len) return null;
    return hexDecode(run_id[dash_pos + 1 ..]);
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
    const now: u64 = @bitCast(std.time.milliTimestamp());
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
        const id = try gen.next(.workflow, pid, &buf);
        const extracted = extractPartition(id).?;
        try std.testing.expectEqual(pid, extracted);
    }
}

test "run IDs are unique" {
    var gen = Generator{};
    var ids: [10]u64 = undefined;
    for (&ids) |*slot| {
        var buf: [32]u8 = undefined;
        const id = try gen.next(.workflow, 100, &buf);
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

    const wf = try gen.next(.workflow, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, wf, "wfr-"));

    const act = try gen.next(.action, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, act, "act-"));

    const job = try gen.next(.job, 0, &buf);
    try std.testing.expect(std.mem.startsWith(u8, job, "job-"));

    const sp = try gen.next(.savepoint, 0, &buf);
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
    const id = try gen.next(.workflow, 777, &buf);

    const req = proto.Request{
        .header = std.mem.zeroes(proto.RequestHeader),
        .namespace = "default",
        .key = id,
        .value = "",
    };

    const hash = preRouteByRunId(req).?;
    try std.testing.expectEqual(@as(u64, 777), hash);
}
