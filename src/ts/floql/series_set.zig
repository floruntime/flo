/// SeriesSet — the universal intermediate representation for FloQL pipelines.
///
/// Every pipeline stage takes a SeriesSet as input and produces a SeriesSet.
/// This is the "row set" equivalent for time-series: a collection of named
/// series, each containing time-ordered data points.
const std = @import("std");
const Allocator = std.mem.Allocator;

/// A single time-series data point.
pub const DataPoint = struct {
    timestamp_ms: i64,
    value: f64,
};

/// A named series with its data points.
pub const Series = struct {
    /// Canonical identity string (e.g., "cpu_usage,host=web-01")
    key: []const u8,
    /// Field name (e.g., "value", "user", "system")
    field: []const u8,
    /// Time-ordered data points
    points: []DataPoint,
    /// Optional metadata tags for group_by operations
    tags: []const Tag = &.{},
    /// Whether key was heap-allocated and should be freed by deinit.
    /// Stages that synthesize new keys (e.g., group_by) set this to true.
    key_owned: bool = false,

    pub const Tag = struct {
        key: []const u8,
        value: []const u8,
    };
};

/// A collection of series flowing through the pipeline.
/// Owns its data — caller must call deinit() when done.
pub const SeriesSet = struct {
    series: []Series,
    allocator: Allocator,

    pub fn init(allocator: Allocator) SeriesSet {
        return .{
            .series = &.{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SeriesSet) void {
        for (self.series) |s| {
            if (s.points.len > 0) self.allocator.free(s.points);
            if (s.tags.len > 0) self.allocator.free(s.tags);
            if (s.key_owned) self.allocator.free(s.key);
        }
        if (self.series.len > 0) self.allocator.free(self.series);
        self.series = &.{};
    }

    /// Create a SeriesSet with pre-allocated series from an owned slice.
    pub fn fromOwned(allocator: Allocator, series: []Series) SeriesSet {
        return .{
            .series = series,
            .allocator = allocator,
        };
    }

    /// Total number of data points across all series.
    pub fn totalPoints(self: SeriesSet) usize {
        var total: usize = 0;
        for (self.series) |s| {
            total += s.points.len;
        }
        return total;
    }

    // ========================================================================
    // Encoding — wire-compatible binary format
    // ========================================================================

    /// Encode to binary for wire response.
    ///
    /// Format:
    ///   [series_count: u32]
    ///   per series:
    ///     [key_len: u32][key: bytes]
    ///     [field_len: u32][field: bytes]
    ///     [point_count: u32]
    ///     per point:
    ///       [timestamp_ms: i64][value: f64]
    pub fn encode(self: SeriesSet, allocator: Allocator) ![]u8 {
        var size: usize = 4; // series_count
        for (self.series) |s| {
            size += 4 + s.key.len; // key_len + key
            size += 4 + s.field.len; // field_len + field
            size += 4; // point_count
            size += s.points.len * 16; // per point: i64 + f64
        }

        const buf = try allocator.alloc(u8, size);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.series.len), .little);
        offset += 4;

        for (self.series) |s| {
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(s.key.len), .little);
            offset += 4;
            @memcpy(buf[offset..][0..s.key.len], s.key);
            offset += s.key.len;

            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(s.field.len), .little);
            offset += 4;
            @memcpy(buf[offset..][0..s.field.len], s.field);
            offset += s.field.len;

            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(s.points.len), .little);
            offset += 4;

            for (s.points) |p| {
                std.mem.writeInt(i64, buf[offset..][0..8], p.timestamp_ms, .little);
                offset += 8;
                @memcpy(buf[offset..][0..8], std.mem.asBytes(&p.value));
                offset += 8;
            }
        }

        return buf;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "series_set_empty" {
    const allocator = std.testing.allocator;
    var ss = SeriesSet.init(allocator);
    defer ss.deinit();

    try std.testing.expectEqual(@as(usize, 0), ss.series.len);
    try std.testing.expectEqual(@as(usize, 0), ss.totalPoints());
}

test "series_set_encode_empty" {
    const allocator = std.testing.allocator;
    var ss = SeriesSet.init(allocator);
    defer ss.deinit();

    const encoded = try ss.encode(allocator);
    defer allocator.free(encoded);

    try std.testing.expectEqual(@as(usize, 4), encoded.len);
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, encoded[0..4], .little));
}

test "series_set_encode_single_series" {
    const allocator = std.testing.allocator;

    var points = try allocator.alloc(DataPoint, 2);
    points[0] = .{ .timestamp_ms = 1000, .value = 42.5 };
    points[1] = .{ .timestamp_ms = 2000, .value = 43.1 };

    var series = try allocator.alloc(Series, 1);
    series[0] = .{
        .key = "cpu,host=web-01",
        .field = "value",
        .points = points,
    };

    var ss = SeriesSet.fromOwned(allocator, series);
    defer ss.deinit();

    try std.testing.expectEqual(@as(usize, 2), ss.totalPoints());

    const encoded = try ss.encode(allocator);
    defer allocator.free(encoded);

    // Verify series count
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, encoded[0..4], .little));
}
