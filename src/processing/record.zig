//! Processing Record Types
//!
//! ProcessingRecord, the unit of data a pipeline's operators pass along.

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
