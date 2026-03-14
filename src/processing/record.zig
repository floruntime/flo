//! Processing Record Types
//!
//! Core data types flowing through the processing DAG:
//! - ProcessingRecord: The fundamental unit of data
//! - Watermark: Event-time progress signal
//! - CheckpointBarrier: Fault-tolerance barrier (Chandy-Lamport)
//! - StreamElement: Tagged union of all element types
//!
//! Design notes:
//! - ProcessingRecord carries event_time_ms from StreamID.timestamp_ms
//! - Watermarks propagate the minimum timestamp guarantee through the DAG
//! - CheckpointBarriers flow in-band, separating records into checkpoint epochs

const std = @import("std");
const Allocator = std.mem.Allocator;
const StreamID = @import("../stream/stream_id.zig").StreamID;

// =============================================================================
// Header - Metadata key-value pair attached to records
// =============================================================================

pub const Header = struct {
    name: []const u8,
    value: []const u8,

    pub fn deinit(self: *const Header, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.value);
    }

    pub fn clone(self: *const Header, allocator: Allocator) !Header {
        return .{
            .name = try allocator.dupe(u8, self.name),
            .value = try allocator.dupe(u8, self.value),
        };
    }
};

// =============================================================================
// SourceRef - Tracks origin stream and offset for checkpoint tracking
// =============================================================================

pub const SourceRef = struct {
    /// Source topic name
    topic: []const u8,
    /// Source partition
    partition: u16,
    /// Offset in source stream (for checkpoint rewinding)
    offset: StreamID,

    pub const EMPTY: SourceRef = .{
        .topic = &.{},
        .partition = 0,
        .offset = StreamID.MIN,
    };

    pub fn clone(self: *const SourceRef, allocator: Allocator) !SourceRef {
        return .{
            .topic = try allocator.dupe(u8, self.topic),
            .partition = self.partition,
            .offset = self.offset,
        };
    }

    pub fn deinit(self: *const SourceRef, allocator: Allocator) void {
        if (self.topic.len > 0) allocator.free(self.topic);
    }
};

// =============================================================================
// ProcessingRecord - The fundamental data unit in the processing pipeline
// =============================================================================

/// A record flowing through the processing pipeline.
///
/// Records are the primary data element that operators transform.
/// Each record has an optional key (for partitioning), a value (payload),
/// and an event timestamp used for windowing and watermark tracking.
///
/// Records carry a `tags` bitfield (u32) that operators can set to label
/// records for downstream routing. Sinks declare which tags they require;
/// a record reaches a sink only if `record.tags & sink_required == sink_required`.
/// Up to 32 distinct tag names per pipeline. Zero-cost when unused.
pub const ProcessingRecord = struct {
    /// Key for partitioning (empty = broadcast/non-keyed)
    key: []const u8,
    /// Payload bytes
    value: []const u8,
    /// Event timestamp in milliseconds (from StreamID or user-assigned)
    event_time_ms: i64,
    /// Source stream and offset for checkpoint tracking
    source: SourceRef,
    /// Optional headers/metadata
    headers: []const Header,
    /// Whether this record owns its memory (and should free on deinit)
    owns_memory: bool,
    /// Tag bitfield — operators set bits to label records for sink routing.
    /// Bit positions are assigned by the pipeline's tag registry.
    tags: u32 = 0,

    pub const EMPTY: ProcessingRecord = .{
        .key = &.{},
        .value = &.{},
        .event_time_ms = 0,
        .source = SourceRef.EMPTY,
        .headers = &.{},
        .owns_memory = false,
        .tags = 0,
    };

    /// Create a record with key and value (borrows memory)
    pub fn init(key: []const u8, value: []const u8, event_time_ms: i64) ProcessingRecord {
        return .{
            .key = key,
            .value = value,
            .event_time_ms = event_time_ms,
            .source = SourceRef.EMPTY,
            .headers = &.{},
            .owns_memory = false,
            .tags = 0,
        };
    }

    /// Create a record from value only (no key, no headers)
    pub fn fromValue(value: []const u8, event_time_ms: i64) ProcessingRecord {
        return .{
            .key = &.{},
            .value = value,
            .event_time_ms = event_time_ms,
            .source = SourceRef.EMPTY,
            .headers = &.{},
            .owns_memory = false,
            .tags = 0,
        };
    }

    /// Deep clone the record, allocating all data
    pub fn clone(self: *const ProcessingRecord, allocator: Allocator) !ProcessingRecord {
        var cloned_headers: []Header = &.{};
        if (self.headers.len > 0) {
            const h = try allocator.alloc(Header, self.headers.len);
            for (self.headers, 0..) |hdr, i| {
                h[i] = try hdr.clone(allocator);
            }
            cloned_headers = h;
        }

        return .{
            .key = if (self.key.len > 0) try allocator.dupe(u8, self.key) else &.{},
            .value = if (self.value.len > 0) try allocator.dupe(u8, self.value) else &.{},
            .event_time_ms = self.event_time_ms,
            .source = try self.source.clone(allocator),
            .headers = cloned_headers,
            .owns_memory = true,
            .tags = self.tags,
        };
    }

    /// Free owned memory
    pub fn deinit(self: *const ProcessingRecord, allocator: Allocator) void {
        if (!self.owns_memory) return;

        if (self.key.len > 0) allocator.free(self.key);
        if (self.value.len > 0) allocator.free(self.value);
        self.source.deinit(allocator);
        for (self.headers) |hdr| {
            hdr.deinit(allocator);
        }
        if (self.headers.len > 0) allocator.free(self.headers);
    }

    /// Check if this record has a key (is keyed)
    pub fn isKeyed(self: *const ProcessingRecord) bool {
        return self.key.len > 0;
    }

    // =========================================================================
    // Tag helpers
    // =========================================================================

    /// Set a tag bit on this record.
    pub fn addTag(self: *ProcessingRecord, bit: u5) void {
        self.tags |= @as(u32, 1) << bit;
    }

    /// Check whether a specific tag bit is set.
    pub fn hasTag(self: *const ProcessingRecord, bit: u5) bool {
        return (self.tags & (@as(u32, 1) << bit)) != 0;
    }

    /// Check whether ALL bits in `mask` are set (AND match).
    pub fn hasAllTags(self: *const ProcessingRecord, mask: u32) bool {
        return (self.tags & mask) == mask;
    }
};

// =============================================================================
// Watermark - Event-time progress signal
// =============================================================================

/// Watermark signals that no records with timestamp <= this will arrive.
///
/// Follows Flink's watermark model:
/// - Generated at sources based on WatermarkStrategy
/// - Propagated through the DAG (multi-input takes minimum)
/// - Triggers window evaluation when watermark passes window end
pub const Watermark = struct {
    /// Event time up to which all records have been observed
    timestamp_ms: i64,
    /// Source index for multi-input watermark alignment
    source_index: u16,

    /// Sentinel: no watermark yet
    pub const NONE: i64 = std.math.minInt(i64);

    pub fn init(timestamp_ms: i64) Watermark {
        return .{ .timestamp_ms = timestamp_ms, .source_index = 0 };
    }

    pub fn withSource(timestamp_ms: i64, source_index: u16) Watermark {
        return .{ .timestamp_ms = timestamp_ms, .source_index = source_index };
    }
};

// =============================================================================
// CheckpointBarrier - Fault-tolerance barrier (Chandy-Lamport)
// =============================================================================

/// Checkpoint barrier — injected at sources, flows through the DAG.
///
/// When an operator receives a barrier from ALL its inputs, it snapshots
/// its state and forwards the barrier downstream. This is the Chandy-Lamport
/// distributed snapshot protocol adapted from Apache Flink.
pub const CheckpointBarrier = struct {
    /// Globally unique checkpoint ID (monotonic)
    checkpoint_id: u64,
    /// Timestamp when checkpoint was initiated
    timestamp_ms: i64,
    /// Whether this is a full or incremental checkpoint
    checkpoint_type: CheckpointType,

    pub const CheckpointType = enum(u8) {
        full = 0,
        incremental = 1,
    };

    pub fn init(checkpoint_id: u64, timestamp_ms: i64) CheckpointBarrier {
        return .{
            .checkpoint_id = checkpoint_id,
            .timestamp_ms = timestamp_ms,
            .checkpoint_type = .full,
        };
    }
};

// =============================================================================
// StreamElement - Tagged union of all element types in the DAG
// =============================================================================

/// Element flowing through the operator DAG.
///
/// The processing pipeline carries four types of elements:
/// - record: Actual data to transform
/// - watermark: Event-time progress marker
/// - barrier: Checkpoint boundary marker
/// - end_of_stream: Graceful shutdown / bounded source completion
pub const StreamElement = union(enum) {
    record: ProcessingRecord,
    watermark: Watermark,
    barrier: CheckpointBarrier,
    end_of_stream: void,

    pub fn isRecord(self: StreamElement) bool {
        return self == .record;
    }

    pub fn isWatermark(self: StreamElement) bool {
        return self == .watermark;
    }

    pub fn isBarrier(self: StreamElement) bool {
        return self == .barrier;
    }

    pub fn deinit(self: *const StreamElement, allocator: Allocator) void {
        switch (self.*) {
            .record => |rec| rec.deinit(allocator),
            else => {},
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "ProcessingRecord.init creates borrowed record" {
    const rec = ProcessingRecord.init("user:42", "hello", 1000);
    try std.testing.expectEqualStrings("user:42", rec.key);
    try std.testing.expectEqualStrings("hello", rec.value);
    try std.testing.expectEqual(@as(i64, 1000), rec.event_time_ms);
    try std.testing.expect(!rec.owns_memory);
    try std.testing.expect(rec.isKeyed());
}

test "ProcessingRecord.fromValue creates non-keyed record" {
    const rec = ProcessingRecord.fromValue("payload", 2000);
    try std.testing.expectEqualStrings("payload", rec.value);
    try std.testing.expect(!rec.isKeyed());
}

test "ProcessingRecord.clone deep copies" {
    const allocator = std.testing.allocator;

    const original = ProcessingRecord.init("key1", "value1", 5000);
    const cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    try std.testing.expectEqualStrings("key1", cloned.key);
    try std.testing.expectEqualStrings("value1", cloned.value);
    try std.testing.expect(cloned.owns_memory);
    // Ensure they are different memory locations
    try std.testing.expect(cloned.key.ptr != original.key.ptr);
}

test "Watermark basic" {
    const wm = Watermark.init(42000);
    try std.testing.expectEqual(@as(i64, 42000), wm.timestamp_ms);
    try std.testing.expectEqual(@as(u16, 0), wm.source_index);
}

test "CheckpointBarrier basic" {
    const barrier = CheckpointBarrier.init(7, 90000);
    try std.testing.expectEqual(@as(u64, 7), barrier.checkpoint_id);
    try std.testing.expectEqual(@as(i64, 90000), barrier.timestamp_ms);
    try std.testing.expectEqual(CheckpointBarrier.CheckpointType.full, barrier.checkpoint_type);
}

test "StreamElement tagged union" {
    const rec_elem = StreamElement{ .record = ProcessingRecord.init("k", "v", 1) };
    try std.testing.expect(rec_elem.isRecord());
    try std.testing.expect(!rec_elem.isWatermark());

    const wm_elem = StreamElement{ .watermark = Watermark.init(100) };
    try std.testing.expect(wm_elem.isWatermark());
    try std.testing.expect(!wm_elem.isRecord());

    const barrier_elem = StreamElement{ .barrier = CheckpointBarrier.init(1, 200) };
    try std.testing.expect(barrier_elem.isBarrier());

    const eos = StreamElement{ .end_of_stream = {} };
    try std.testing.expect(!eos.isRecord());
}
