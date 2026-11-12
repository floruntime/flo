//! Processing Record Types
//!
//! ProcessingRecord, the unit of data a pipeline's operators pass along.
//!
//! A record owns nothing. What it points at is the chain's input, which
//! outlives the chain, or what an operator allocated from its context's
//! allocator, an arena the caller frees once it has written that input's
//! outputs (`OperatorContext.allocator`). So a record passed through, or
//! one aliasing another's bytes, never outlives what it points at, and
//! nothing along the chain frees a record.

const std = @import("std");
const Allocator = std.mem.Allocator;
const StreamID = @import("../stream/stream_id.zig").StreamID;

// =============================================================================
// Header - Metadata key-value pair attached to records
// =============================================================================

pub const Header = struct {
    name: []const u8,
    value: []const u8,
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
    /// Tag bitfield — operators set bits to label records for sink routing.
    /// Bit positions are assigned by the pipeline's tag registry.
    tags: u32 = 0,

    pub const EMPTY: ProcessingRecord = .{
        .key = &.{},
        .value = &.{},
        .event_time_ms = 0,
        .source = SourceRef.EMPTY,
        .headers = &.{},
        .tags = 0,
    };

    /// A record with key and value
    pub fn init(key: []const u8, value: []const u8, event_time_ms: i64) ProcessingRecord {
        return .{
            .key = key,
            .value = value,
            .event_time_ms = event_time_ms,
            .source = SourceRef.EMPTY,
            .headers = &.{},
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
            .tags = 0,
        };
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
// Tests
// =============================================================================

test "ProcessingRecord.init creates borrowed record" {
    const rec = ProcessingRecord.init("user:42", "hello", 1000);
    try std.testing.expectEqualStrings("user:42", rec.key);
    try std.testing.expectEqualStrings("hello", rec.value);
    try std.testing.expectEqual(@as(i64, 1000), rec.event_time_ms);
    try std.testing.expect(rec.isKeyed());
}

test "ProcessingRecord.fromValue creates non-keyed record" {
    const rec = ProcessingRecord.fromValue("payload", 2000);
    try std.testing.expectEqualStrings("payload", rec.value);
    try std.testing.expect(!rec.isKeyed());
}
