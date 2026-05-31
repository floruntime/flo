//! Stream ID - Timestamp-Sequence Identifiers
//!
//! StreamID provides unique, time-ordered identifiers with the format:
//! `<milliseconds>-<sequence>` (e.g., "1703350800000-3")
//!
//! ## Design
//!
//! Each StreamID is 128 bits composed of:
//! - **timestamp_ms** (u64): Unix milliseconds since epoch
//! - **sequence** (u64): Counter for multiple entries in same millisecond
//!
//! ## Features
//!
//! - **Time-based seeking**: Query "all messages after 3pm" naturally
//! - **Human readable**: Timestamps are debuggable
//! - **Monotonic**: IDs always increase, even with clock skew
//! - **Lexicographic**: Binary form sorts correctly via big-endian encoding
//!
//! ## Clock Skew Handling
//!
//! If system clock goes backward, we use previous timestamp + increment sequence.
//! This guarantees monotonicity without coordination.
//!
//! ## Usage
//!
//! ```zig
//! var gen = StreamIdGenerator.init();
//!
//! // Generate IDs (monotonically increasing)
//! const id1 = gen.next();  // 1703350800000-0
//! const id2 = gen.next();  // 1703350800000-1 (same ms)
//! // ... time passes ...
//! const id3 = gen.next();  // 1703350801000-0 (new ms)
//!
//! // Parse from string
//! const parsed = try StreamID.parse("1703350800000-5");
//!
//! // Compare
//! if (id1.lessThan(id2)) { ... }
//!
//! // Seek by timestamp
//! const start = StreamID.fromTimestamp(1703350800000);  // 1703350800000-0
//! ```

const std = @import("std");
const mem = std.mem;

/// Stream ID - 128-bit timestamp-sequence identifier
///
/// Binary layout (big-endian for lexicographic sorting):
/// ```
/// [timestamp_ms: u64 BE][sequence: u64 BE]
/// ```
pub const StreamID = struct {
    /// Unix timestamp in milliseconds
    timestamp_ms: u64,
    /// Sequence number within the millisecond (0-indexed)
    sequence: u64,

    /// Size of binary-encoded StreamID
    pub const SIZE: usize = 16;

    /// Minimum possible ID (for range scans)
    pub const MIN = StreamID{ .timestamp_ms = 0, .sequence = 0 };

    /// Maximum possible ID (for range scans)
    pub const MAX = StreamID{ .timestamp_ms = std.math.maxInt(u64), .sequence = std.math.maxInt(u64) };

    /// Create a StreamID from sequence only (timestamp = 0)
    /// Used for sequence-based range scans and info responses.
    pub fn fromSeq(seq: u64) StreamID {
        return .{ .timestamp_ms = 0, .sequence = seq };
    }

    /// Create a StreamID from timestamp only (sequence = 0)
    /// Useful for seeking to "start of this millisecond"
    pub fn fromTimestamp(timestamp_ms: u64) StreamID {
        return .{ .timestamp_ms = timestamp_ms, .sequence = 0 };
    }

    /// Create a StreamID from timestamp with max sequence
    /// Useful for seeking to "end of this millisecond"
    pub fn fromTimestampMax(timestamp_ms: u64) StreamID {
        return .{ .timestamp_ms = timestamp_ms, .sequence = std.math.maxInt(u64) };
    }

    /// Parse StreamID from string format "timestamp-sequence"
    /// Examples: "1703350800000-0", "1703350800000-42"
    pub fn parse(str: []const u8) !StreamID {
        // Handle special cases
        if (str.len == 0) return error.InvalidStreamID;

        // Special: "$" means "latest" - return MAX as sentinel
        if (mem.eql(u8, str, "$")) {
            return MAX;
        }

        // Special: "0" or "0-0" means "from beginning"
        if (mem.eql(u8, str, "0") or mem.eql(u8, str, "0-0")) {
            return MIN;
        }

        // Find the dash separator
        const dash_pos = mem.indexOf(u8, str, "-") orelse {
            // No dash - treat as timestamp only
            const timestamp = std.fmt.parseInt(u64, str, 10) catch return error.InvalidStreamID;
            return fromTimestamp(timestamp);
        };

        // Parse timestamp (before dash)
        const timestamp_str = str[0..dash_pos];
        const timestamp = std.fmt.parseInt(u64, timestamp_str, 10) catch return error.InvalidStreamID;

        // Parse sequence (after dash)
        const seq_str = str[dash_pos + 1 ..];
        if (seq_str.len == 0) return error.InvalidStreamID;

        const sequence = std.fmt.parseInt(u64, seq_str, 10) catch return error.InvalidStreamID;

        return .{ .timestamp_ms = timestamp, .sequence = sequence };
    }

    /// Format as string "timestamp-sequence"
    pub fn format(self: StreamID, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "{d}-{d}", .{ self.timestamp_ms, self.sequence }) catch error.BufferTooSmall;
    }

    /// Format as string, allocating result
    pub fn toString(self: StreamID, allocator: mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "{d}-{d}", .{ self.timestamp_ms, self.sequence });
    }

    /// Compare two StreamIDs
    pub fn compare(a: StreamID, b: StreamID) std.math.Order {
        if (a.timestamp_ms < b.timestamp_ms) return .lt;
        if (a.timestamp_ms > b.timestamp_ms) return .gt;
        if (a.sequence < b.sequence) return .lt;
        if (a.sequence > b.sequence) return .gt;
        return .eq;
    }

    /// Get a u128 ordering value for use in comparisons and hash maps
    /// Combines timestamp_ms and sequence into a single sortable value
    pub fn order(self: StreamID) u128 {
        return (@as(u128, self.timestamp_ms) << 64) | @as(u128, self.sequence);
    }

    /// Inverse of `order` — reconstruct a StreamID from its u128 ordering key.
    pub fn fromOrder(key: u128) StreamID {
        return .{
            .timestamp_ms = @intCast(key >> 64),
            .sequence = @intCast(key & std.math.maxInt(u64)),
        };
    }

    /// Check if this ID is less than another
    pub fn lessThan(self: StreamID, other: StreamID) bool {
        return self.compare(other) == .lt;
    }

    /// Check if this ID is less than or equal to another
    pub fn lessThanOrEqual(self: StreamID, other: StreamID) bool {
        return self.compare(other) != .gt;
    }

    /// Check if this ID is greater than another
    pub fn greaterThan(self: StreamID, other: StreamID) bool {
        return self.compare(other) == .gt;
    }

    /// Check equality
    pub fn eql(self: StreamID, other: StreamID) bool {
        return self.timestamp_ms == other.timestamp_ms and self.sequence == other.sequence;
    }

    /// Encode to binary (big-endian for lexicographic sorting) - writes to provided buffer
    pub fn encode(self: StreamID, buf: *[SIZE]u8) void {
        mem.writeInt(u64, buf[0..8], self.timestamp_ms, .big);
        mem.writeInt(u64, buf[8..16], self.sequence, .big);
    }

    /// Encode to binary and return as array (for inline use)
    pub fn toBytes(self: StreamID) [SIZE]u8 {
        var buf: [SIZE]u8 = undefined;
        self.encode(&buf);
        return buf;
    }

    /// Decode from binary
    pub fn decode(buf: *const [SIZE]u8) StreamID {
        return .{
            .timestamp_ms = mem.readInt(u64, buf[0..8], .big),
            .sequence = mem.readInt(u64, buf[8..16], .big),
        };
    }

    /// Get next ID in sequence (same timestamp, seq + 1)
    pub fn next(self: StreamID) StreamID {
        return .{
            .timestamp_ms = self.timestamp_ms,
            .sequence = self.sequence + 1,
        };
    }

    /// Increment to next valid ID (for exclusive range starts)
    /// If sequence is max, increment timestamp
    pub fn increment(self: StreamID) StreamID {
        if (self.sequence == std.math.maxInt(u64)) {
            return .{
                .timestamp_ms = self.timestamp_ms + 1,
                .sequence = 0,
            };
        }
        return self.next();
    }
};

/// Generator for monotonically increasing StreamIDs
///
/// Handles:
/// - Multiple appends in same millisecond (incrementing sequence)
/// - Clock skew (uses previous timestamp if clock goes backward)
///
/// Thread-safe via atomic operations.
pub const StreamIdGenerator = struct {
    /// Last generated timestamp
    last_timestamp_ms: std.atomic.Value(u64),
    /// Last sequence number in that timestamp
    last_sequence: std.atomic.Value(u64),

    /// Initialize generator (typically from persisted state)
    pub fn init() StreamIdGenerator {
        return .{
            .last_timestamp_ms = std.atomic.Value(u64).init(0),
            .last_sequence = std.atomic.Value(u64).init(0),
        };
    }

    /// Initialize from a known last ID (for recovery)
    pub fn initFrom(last_id: StreamID) StreamIdGenerator {
        return .{
            .last_timestamp_ms = std.atomic.Value(u64).init(last_id.timestamp_ms),
            .last_sequence = std.atomic.Value(u64).init(last_id.sequence),
        };
    }

    /// Generate next StreamID using the current wall clock.
    ///
    /// Guarantees:
    /// - Monotonically increasing (never generates duplicate or lower ID)
    /// - Clock skew safe (if clock goes backward, uses previous timestamp + seq)
    ///
    /// NOTE: durable apply paths must use `nextAt` with the originating UAL
    /// entry's timestamp instead, so replay reproduces identical StreamIDs
    /// (FLO-103). Reading the wall clock here would renumber records on restart.
    pub fn next(self: *StreamIdGenerator) StreamID {
        return self.nextAt(@as(u64, @intCast(@import("stdx").time.milliTimestamp())));
    }

    /// Generate the next StreamID anchored to an explicit timestamp (the UAL
    /// entry's `header.timestamp_ns`, in ms). Deterministic across replay:
    /// applying the same entries in index order reproduces identical IDs, so a
    /// persisted consumer-group cursor still lines up after restart (FLO-103).
    pub fn nextAt(self: *StreamIdGenerator, now_ms: u64) StreamID {
        // Load current state
        const last_ts = self.last_timestamp_ms.load(.acquire);
        const last_seq = self.last_sequence.load(.acquire);

        // Determine new ID
        var new_ts: u64 = undefined;
        var new_seq: u64 = undefined;

        if (now_ms > last_ts) {
            // Time has advanced - new timestamp, reset sequence
            new_ts = now_ms;
            new_seq = 0;
        } else {
            // Same millisecond or clock went backward - increment sequence
            new_ts = last_ts;
            new_seq = last_seq + 1;

            // Handle sequence overflow (extremely unlikely - would need 2^64 writes/ms)
            if (new_seq == 0) {
                new_ts += 1;
            }
        }

        // Update state atomically
        // Note: In high-contention scenarios, this could be optimized with CAS loop
        self.last_timestamp_ms.store(new_ts, .release);
        self.last_sequence.store(new_seq, .release);

        return .{
            .timestamp_ms = new_ts,
            .sequence = new_seq,
        };
    }

    /// First and last ID produced by a batch allocation.
    pub const Batch = struct { first: StreamID, last: StreamID };

    /// Generate multiple IDs atomically (for batch appends)
    pub fn nextBatch(self: *StreamIdGenerator, count: usize) !Batch {
        return self.nextBatchAt(@as(u64, @intCast(@import("stdx").time.milliTimestamp())), count);
    }

    /// Batch variant of `nextAt` — anchors the batch to an explicit timestamp
    /// for deterministic replay (FLO-103).
    pub fn nextBatchAt(self: *StreamIdGenerator, now_ms: u64, count: usize) !Batch {
        if (count == 0) return error.EmptyBatch;

        // Load current state
        const last_ts = self.last_timestamp_ms.load(.acquire);
        const last_seq = self.last_sequence.load(.acquire);

        // Determine range
        var first_ts: u64 = undefined;
        var first_seq: u64 = undefined;

        if (now_ms > last_ts) {
            first_ts = now_ms;
            first_seq = 0;
        } else {
            first_ts = last_ts;
            first_seq = last_seq + 1;
        }

        // Calculate last ID in batch
        const last_seq_in_batch = first_seq + count - 1;

        // Update state
        self.last_timestamp_ms.store(first_ts, .release);
        self.last_sequence.store(last_seq_in_batch, .release);

        return .{
            .first = .{ .timestamp_ms = first_ts, .sequence = first_seq },
            .last = .{ .timestamp_ms = first_ts, .sequence = last_seq_in_batch },
        };
    }

    /// Get current last ID (without generating new one)
    pub fn current(self: *const StreamIdGenerator) StreamID {
        return .{
            .timestamp_ms = self.last_timestamp_ms.load(.acquire),
            .sequence = self.last_sequence.load(.acquire),
        };
    }

    /// Reset to a specific ID (for testing or recovery)
    pub fn reset(self: *StreamIdGenerator, id: StreamID) void {
        self.last_timestamp_ms.store(id.timestamp_ms, .release);
        self.last_sequence.store(id.sequence, .release);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "StreamID.parse basic" {
    const id = try StreamID.parse("1703350800000-5");
    try std.testing.expectEqual(@as(u64, 1703350800000), id.timestamp_ms);
    try std.testing.expectEqual(@as(u64, 5), id.sequence);
}

test "StreamID.parse timestamp only" {
    const id = try StreamID.parse("1703350800000");
    try std.testing.expectEqual(@as(u64, 1703350800000), id.timestamp_ms);
    try std.testing.expectEqual(@as(u64, 0), id.sequence);
}

test "StreamID.parse special values" {
    // "$" = MAX (latest)
    const latest = try StreamID.parse("$");
    try std.testing.expectEqual(StreamID.MAX, latest);

    // "0" = MIN (beginning)
    const min = try StreamID.parse("0");
    try std.testing.expectEqual(StreamID.MIN, min);

    // "0-0" = MIN
    const min2 = try StreamID.parse("0-0");
    try std.testing.expectEqual(StreamID.MIN, min2);
}

test "StreamID.compare" {
    const a = StreamID{ .timestamp_ms = 1000, .sequence = 5 };
    const b = StreamID{ .timestamp_ms = 1000, .sequence = 10 };
    const c = StreamID{ .timestamp_ms = 2000, .sequence = 0 };

    try std.testing.expect(a.lessThan(b));
    try std.testing.expect(b.lessThan(c));
    try std.testing.expect(a.lessThan(c));
    try std.testing.expect(!c.lessThan(a));
}

test "StreamID.encode_decode roundtrip" {
    const original = StreamID{ .timestamp_ms = 1703350800000, .sequence = 42 };
    var buf: [StreamID.SIZE]u8 = undefined;
    original.encode(&buf);

    const decoded = StreamID.decode(&buf);
    try std.testing.expectEqual(original.timestamp_ms, decoded.timestamp_ms);
    try std.testing.expectEqual(original.sequence, decoded.sequence);
}

test "StreamID.format" {
    const id = StreamID{ .timestamp_ms = 1703350800000, .sequence = 3 };
    var buf: [64]u8 = undefined;
    const str = try id.format(&buf);
    try std.testing.expectEqualStrings("1703350800000-3", str);
}

test "StreamIdGenerator monotonic" {
    var gen = StreamIdGenerator.init();

    const id1 = gen.next();
    const id2 = gen.next();
    const id3 = gen.next();

    // Each ID should be greater than the previous
    try std.testing.expect(id1.lessThan(id2));
    try std.testing.expect(id2.lessThan(id3));
}

test "StreamIdGenerator nextAt is deterministic across replay (FLO-103)" {
    // Two generators fed the SAME per-entry timestamps must produce identical
    // IDs, regardless of wall clock — this is what lets replay reproduce the
    // original StreamIDs so a persisted consumer-group cursor still lines up.
    const ts = [_]u64{ 1000, 1000, 1000, 1002, 1002, 1005 };

    var live = StreamIdGenerator.init();
    var replay = StreamIdGenerator.init();

    for (ts) |t| {
        const a = live.nextAt(t);
        const b = replay.nextAt(t);
        try std.testing.expectEqual(a.timestamp_ms, b.timestamp_ms);
        try std.testing.expectEqual(a.sequence, b.sequence);
    }

    // Same-ms entries get incrementing sequences; later ms resets sequence.
    var g = StreamIdGenerator.init();
    const a0 = g.nextAt(1000);
    const a1 = g.nextAt(1000);
    const a2 = g.nextAt(1002);
    try std.testing.expectEqual(@as(u64, 0), a0.sequence);
    try std.testing.expectEqual(@as(u64, 1), a1.sequence);
    try std.testing.expectEqual(@as(u64, 1002), a2.timestamp_ms);
    try std.testing.expectEqual(@as(u64, 0), a2.sequence);
}

test "StreamIdGenerator batch" {
    var gen = StreamIdGenerator.init();

    const batch = try gen.nextBatch(5);

    try std.testing.expectEqual(batch.first.timestamp_ms, batch.last.timestamp_ms);
    try std.testing.expectEqual(batch.first.sequence + 4, batch.last.sequence);

    // Next single ID should be after batch
    const next_single = gen.next();
    try std.testing.expect(batch.last.lessThan(next_single));
}

test "StreamIdGenerator recovery" {
    const saved_id = StreamID{ .timestamp_ms = 1703350800000, .sequence = 100 };
    var gen = StreamIdGenerator.initFrom(saved_id);

    const next_id = gen.next();

    // Should be after the saved ID
    try std.testing.expect(saved_id.lessThan(next_id));
}
