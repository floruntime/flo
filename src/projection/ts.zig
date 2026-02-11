//! TimeSeries Projection — columnar write buffers + block index.
//!
//! Stores time-series data in a columnar layout for efficient range queries
//! and aggregation. Points are buffered in a write buffer, then flushed to
//! immutable blocks when the buffer is full.
//!
//! Data model:
//!   - Measurement: top-level namespace (e.g., "cpu", "memory")
//!   - Tags: key-value pairs for filtering (e.g., host=web-01)
//!   - Fields: numeric values (e.g., usage=82.5, idle=17.5)
//!   - Timestamp: nanosecond precision
//!
//! Storage layout:
//!   WriteBuffer (mutable, per-measurement):
//!     - timestamps[]   — sorted column
//!     - field_values[]  — parallel columns per field name
//!     - tag_values[]    — for filtering
//!   Block (immutable, flushed from WriteBuffer):
//!     - min/max timestamp
//!     - point count
//!     - UAL index range
//!
//! Applied via ProjectionRouter from committed UAL entries:
//!   ts_write       → insert single point
//!   ts_write_batch → insert batch of points

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;

// ═══════════════════════════════════════════════════════════════════════════════
// Data Point
// ═══════════════════════════════════════════════════════════════════════════════

pub const FieldValue = union(enum) {
    float: f64,
    int: i64,
    boolean: bool,
};

pub const Tag = struct {
    key: []const u8,
    value: []const u8,
};

pub const DataPoint = struct {
    measurement: []const u8,
    tags: []const Tag,
    field_name: []const u8,
    field_value: FieldValue,
    timestamp_ns: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Stored Point — what we keep in the write buffer
// ═══════════════════════════════════════════════════════════════════════════════

const StoredPoint = struct {
    timestamp_ns: u64,
    field_value: f64, // normalized to f64
    ual_index: u64,
    /// Packed tag string for filtering (e.g., "host=web-01,region=us")
    tag_hash: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Block — immutable, flushed from write buffer
// ═══════════════════════════════════════════════════════════════════════════════

pub const Block = struct {
    min_timestamp_ns: u64,
    max_timestamp_ns: u64,
    point_count: u32,
    ual_index_start: u64,
    ual_index_end: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Write Buffer — per measurement+field
// ═══════════════════════════════════════════════════════════════════════════════

pub const WriteBuffer = struct {
    points: std.ArrayList(StoredPoint),
    allocator: Allocator,
    capacity: usize,

    pub fn init(allocator: Allocator, capacity: usize) WriteBuffer {
        return .{
            .points = .empty,
            .allocator = allocator,
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *WriteBuffer) void {
        self.points.deinit(self.allocator);
    }

    pub fn append(self: *WriteBuffer, point: StoredPoint) !void {
        try self.points.append(self.allocator, point);
    }

    pub fn isFull(self: *const WriteBuffer) bool {
        return self.points.items.len >= self.capacity;
    }

    pub fn len(self: *const WriteBuffer) usize {
        return self.points.items.len;
    }

    pub fn clear(self: *WriteBuffer) void {
        self.points.clearRetainingCapacity();
    }

    /// Query points within a timestamp range [min_ts, max_ts] inclusive.
    pub fn queryRange(self: *const WriteBuffer, min_ts: u64, max_ts: u64, buf: []StoredPoint) usize {
        var count: usize = 0;
        for (self.points.items) |point| {
            if (point.timestamp_ns >= min_ts and point.timestamp_ns <= max_ts) {
                if (count < buf.len) {
                    buf[count] = point;
                    count += 1;
                } else break;
            }
        }
        return count;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Series Key — measurement + field name
// ═══════════════════════════════════════════════════════════════════════════════

const SeriesKey = struct {
    measurement: []const u8,
    field_name: []const u8,
};

fn seriesKeyHash(key: SeriesKey) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(key.measurement);
    hasher.update("\x00");
    hasher.update(key.field_name);
    return hasher.final();
}

fn seriesKeyToString(allocator: Allocator, key: SeriesKey) ![]const u8 {
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, key.measurement);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, key.field_name);
    return try buf.toOwnedSlice(allocator);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TimeSeries Projection
// ═══════════════════════════════════════════════════════════════════════════════

pub const TSProjection = struct {
    allocator: Allocator,

    /// Write buffers keyed by "measurement\x00field_name".
    buffers: std.StringHashMap(WriteBuffer),

    /// Flushed blocks (per series key).
    blocks: std.StringHashMap(std.ArrayList(Block)),

    /// Buffer capacity (points in write buffer before flush).
    buffer_capacity: usize,

    /// Last applied UAL index.
    applied_index: u64,

    /// Stats.
    stats: Stats,

    pub const Stats = struct {
        points_inserted: u64 = 0,
        blocks_flushed: u64 = 0,
        queries: u64 = 0,
    };

    pub const Config = struct {
        buffer_capacity: usize = 1024,
    };

    pub fn init(allocator: Allocator, config: Config) TSProjection {
        return .{
            .allocator = allocator,
            .buffers = std.StringHashMap(WriteBuffer).init(allocator),
            .blocks = std.StringHashMap(std.ArrayList(Block)).init(allocator),
            .buffer_capacity = config.buffer_capacity,
            .applied_index = 0,
            .stats = .{},
        };
    }

    pub fn deinit(self: *TSProjection) void {
        // Free write buffers
        var bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            kv.value_ptr.deinit();
            self.allocator.free(@constCast(kv.key_ptr.*));
        }
        self.buffers.deinit();

        // Free blocks
        var blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            kv.value_ptr.deinit(self.allocator);
            self.allocator.free(@constCast(kv.key_ptr.*));
        }
        self.blocks.deinit();
    }

    // ─── Core operations ───────────────────────────────────────────────────

    /// Insert a single data point.
    pub fn insert(
        self: *TSProjection,
        measurement: []const u8,
        field_name: []const u8,
        value: f64,
        timestamp_ns: u64,
        ual_index: u64,
        tag_hash: u64,
    ) !void {
        const series_str = try seriesKeyToString(self.allocator, .{
            .measurement = measurement,
            .field_name = field_name,
        });

        const gop = try self.buffers.getOrPut(series_str);

        if (gop.found_existing) {
            // Don't need the new key
            self.allocator.free(series_str);
        } else {
            // Store the key and init a new write buffer
            gop.key_ptr.* = series_str;
            gop.value_ptr.* = WriteBuffer.init(self.allocator, self.buffer_capacity);
        }

        const buf = gop.value_ptr;
        try buf.append(.{
            .timestamp_ns = timestamp_ns,
            .field_value = value,
            .ual_index = ual_index,
            .tag_hash = tag_hash,
        });

        self.stats.points_inserted += 1;

        // Check if buffer needs flushing
        if (buf.isFull()) {
            try self.flushBuffer(gop.key_ptr.*, gop.value_ptr);
        }
    }

    fn flushBuffer(self: *TSProjection, series_key: []const u8, buf: *WriteBuffer) !void {
        if (buf.len() == 0) return;

        var min_ts: u64 = std.math.maxInt(u64);
        var max_ts: u64 = 0;
        var min_ual: u64 = std.math.maxInt(u64);
        var max_ual: u64 = 0;

        for (buf.points.items) |p| {
            min_ts = @min(min_ts, p.timestamp_ns);
            max_ts = @max(max_ts, p.timestamp_ns);
            min_ual = @min(min_ual, p.ual_index);
            max_ual = @max(max_ual, p.ual_index);
        }

        const block = Block{
            .min_timestamp_ns = min_ts,
            .max_timestamp_ns = max_ts,
            .point_count = @intCast(buf.len()),
            .ual_index_start = min_ual,
            .ual_index_end = max_ual,
        };

        const gop = try self.blocks.getOrPut(series_key);
        if (!gop.found_existing) {
            // Need to own the key for the blocks map
            const owned_key = try self.allocator.dupe(u8, series_key);
            gop.key_ptr.* = owned_key;
            gop.value_ptr.* = .empty;
        }

        try gop.value_ptr.append(self.allocator, block);

        buf.clear();
        self.stats.blocks_flushed += 1;
    }

    /// Query points in a timestamp range for a given measurement and field.
    /// Returns points from both the write buffer and flushed blocks.
    pub fn queryRange(
        self: *TSProjection,
        measurement: []const u8,
        field_name: []const u8,
        min_ts: u64,
        max_ts: u64,
        point_buf: []StoredPoint,
    ) !QueryResult {
        const series_str = try seriesKeyToString(self.allocator, .{
            .measurement = measurement,
            .field_name = field_name,
        });
        defer self.allocator.free(series_str);

        var result = QueryResult{
            .points_in_buffer = 0,
            .blocks_matched = 0,
        };

        // Query write buffer
        if (self.buffers.get(series_str)) |buf| {
            result.points_in_buffer = buf.queryRange(min_ts, max_ts, point_buf);
        }

        // Count matching blocks
        if (self.blocks.get(series_str)) |block_list| {
            for (block_list.items) |block| {
                if (block.max_timestamp_ns >= min_ts and block.min_timestamp_ns <= max_ts) {
                    result.blocks_matched += 1;
                }
            }
        }

        self.stats.queries += 1;
        return result;
    }

    pub const QueryResult = struct {
        points_in_buffer: usize,
        blocks_matched: usize,
    };

    // ─── Aggregations ──────────────────────────────────────────────────────

    /// Compute the average of points in a range from the write buffer.
    pub fn avg(self: *TSProjection, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(measurement, field_name, min_ts, max_ts, &buf);
        if (result.points_in_buffer == 0) return null;

        var acc: f64 = 0;
        for (buf[0..result.points_in_buffer]) |p| {
            acc += p.field_value;
        }
        return acc / @as(f64, @floatFromInt(result.points_in_buffer));
    }

    /// Compute the min of points in a range from the write buffer.
    pub fn min(self: *TSProjection, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(measurement, field_name, min_ts, max_ts, &buf);
        if (result.points_in_buffer == 0) return null;

        var min_val: f64 = std.math.floatMax(f64);
        for (buf[0..result.points_in_buffer]) |p| {
            min_val = @min(min_val, p.field_value);
        }
        return min_val;
    }

    /// Compute the max of points in a range from the write buffer.
    pub fn max(self: *TSProjection, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(measurement, field_name, min_ts, max_ts, &buf);
        if (result.points_in_buffer == 0) return null;

        var max_val: f64 = -std.math.floatMax(f64);
        for (buf[0..result.points_in_buffer]) |p| {
            max_val = @max(max_val, p.field_value);
        }
        return max_val;
    }

    /// Compute the sum of points in a range from the write buffer.
    pub fn sum(self: *TSProjection, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var point_buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(measurement, field_name, min_ts, max_ts, &point_buf);
        if (result.points_in_buffer == 0) return null;

        var total: f64 = 0;
        for (point_buf[0..result.points_in_buffer]) |p| {
            total += p.field_value;
        }
        return total;
    }

    /// Count points in a range from the write buffer.
    pub fn count(self: *TSProjection, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !usize {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(measurement, field_name, min_ts, max_ts, &buf);
        return result.points_in_buffer;
    }

    // ─── Info ──────────────────────────────────────────────────────────────

    /// Number of active series (measurement+field combos with data).
    pub fn seriesCount(self: *const TSProjection) usize {
        return self.buffers.count();
    }

    /// Total flushed blocks across all series.
    pub fn totalBlocks(self: *const TSProjection) usize {
        var total: usize = 0;
        var it = self.blocks.iterator();
        while (it.next()) |kv| {
            total += kv.value_ptr.items.len;
        }
        return total;
    }

    // ─── UAL Entry application ─────────────────────────────────────────────

    pub fn applyEntry(self: *TSProjection, ual_entry: *const Entry) !void {
        const entry_type: EntryType = @enumFromInt(ual_entry.header.entry_type);

        switch (entry_type) {
            .ts_write, .ts_write_batch => {
                // For now, treat payload as a simple point:
                // measurement name is the key, value is f64 bytes
                if (entry_mod.CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    var value: f64 = 0.0;
                    if (cmd.value.len >= 8) {
                        value = @bitCast(std.mem.readInt(u64, cmd.value[0..8], .little));
                    }
                    // Use measurement as both measurement and field for simplified path
                    try self.insert(
                        cmd.key,
                        "value",
                        value,
                        ual_entry.header.timestamp_ns,
                        ual_entry.header.index,
                        0,
                    );
                }
            },
            else => {},
        }

        self.applied_index = ual_entry.header.index;
    }

    /// ProjectionVTable implementation.
    pub fn projectionHandle(self: *TSProjection) router_mod.ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = vtableApply,
                .memoryUsageFn = vtableMemory,
            },
        };
    }

    fn vtableApply(ctx: *anyopaque, ual_entry: *const Entry) router_mod.ApplyError!void {
        const self: *TSProjection = @ptrCast(@alignCast(ctx));
        self.applyEntry(ual_entry) catch return error.OutOfMemory;
    }

    fn vtableMemory(ctx: *anyopaque) usize {
        const self: *TSProjection = @ptrCast(@alignCast(ctx));
        return self.memoryUsage();
    }

    pub fn memoryUsage(self: *const TSProjection) usize {
        var mem: usize = @sizeOf(TSProjection);

        var bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            mem += kv.key_ptr.len;
            mem += kv.value_ptr.points.items.len * @sizeOf(StoredPoint);
        }

        var blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            mem += kv.key_ptr.len;
            mem += kv.value_ptr.items.len * @sizeOf(Block);
        }

        return mem;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "ts: basic insert and query" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("cpu", "usage", 82.5, 1000, 1, 0);
    try ts.insert("cpu", "usage", 75.0, 2000, 2, 0);
    try ts.insert("cpu", "usage", 90.1, 3000, 3, 0);

    try testing.expectEqual(@as(u64, 3), ts.stats.points_inserted);
    try testing.expectEqual(@as(usize, 1), ts.seriesCount());

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange("cpu", "usage", 1000, 3000, &buf);
    try testing.expectEqual(@as(usize, 3), result.points_in_buffer);
    try testing.expectEqual(@as(f64, 82.5), buf[0].field_value);
}

test "ts: range query filters by timestamp" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("mem", "used", 100.0, 1000, 1, 0);
    try ts.insert("mem", "used", 200.0, 2000, 2, 0);
    try ts.insert("mem", "used", 300.0, 3000, 3, 0);
    try ts.insert("mem", "used", 400.0, 4000, 4, 0);

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange("mem", "used", 2000, 3000, &buf);
    try testing.expectEqual(@as(usize, 2), result.points_in_buffer);
    try testing.expectEqual(@as(f64, 200.0), buf[0].field_value);
    try testing.expectEqual(@as(f64, 300.0), buf[1].field_value);
}

test "ts: aggregation — avg" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("cpu", "usage", 80.0, 1000, 1, 0);
    try ts.insert("cpu", "usage", 90.0, 2000, 2, 0);
    try ts.insert("cpu", "usage", 100.0, 3000, 3, 0);

    const average = (try ts.avg("cpu", "usage", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 90.0), average, 0.001);
}

test "ts: aggregation — min/max/sum" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("disk", "iops", 10.0, 1000, 1, 0);
    try ts.insert("disk", "iops", 50.0, 2000, 2, 0);
    try ts.insert("disk", "iops", 30.0, 3000, 3, 0);

    const min_val = (try ts.min("disk", "iops", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 10.0), min_val, 0.001);

    const max_val = (try ts.max("disk", "iops", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 50.0), max_val, 0.001);

    const sum_val = (try ts.sum("disk", "iops", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 90.0), sum_val, 0.001);
}

test "ts: aggregation — count" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("net", "rx", 1.0, 1000, 1, 0);
    try ts.insert("net", "rx", 2.0, 2000, 2, 0);
    try ts.insert("net", "rx", 3.0, 3000, 3, 0);

    const c = try ts.count("net", "rx", 1500, 2500);
    try testing.expectEqual(@as(usize, 1), c); // only ts=2000 in range
}

test "ts: buffer flush creates block" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 3 });
    defer ts.deinit();

    try ts.insert("cpu", "usage", 1.0, 1000, 1, 0);
    try ts.insert("cpu", "usage", 2.0, 2000, 2, 0);
    try testing.expectEqual(@as(u64, 0), ts.stats.blocks_flushed);

    // Third insert triggers flush
    try ts.insert("cpu", "usage", 3.0, 3000, 3, 0);
    try testing.expectEqual(@as(u64, 1), ts.stats.blocks_flushed);
    try testing.expectEqual(@as(usize, 1), ts.totalBlocks());

    // Buffer should be cleared after flush
    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange("cpu", "usage", 1000, 3000, &buf);
    try testing.expectEqual(@as(usize, 0), result.points_in_buffer);
    try testing.expectEqual(@as(usize, 1), result.blocks_matched);
}

test "ts: multiple series" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("cpu", "usage", 80.0, 1000, 1, 0);
    try ts.insert("cpu", "idle", 20.0, 1000, 2, 0);
    try ts.insert("mem", "used", 4096.0, 1000, 3, 0);

    try testing.expectEqual(@as(usize, 3), ts.seriesCount());
}

test "ts: query nonexistent series" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange("nonexistent", "field", 0, 9999, &buf);
    try testing.expectEqual(@as(usize, 0), result.points_in_buffer);
    try testing.expectEqual(@as(usize, 0), result.blocks_matched);
}

test "ts: memory usage estimate" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert("cpu", "usage", 80.0, 1000, 1, 0);
    try testing.expect(ts.memoryUsage() > 0);
}

test "ts: apply entry for ts_write" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    // Build a ts_write UAL entry with key="cpu" and value=f64 bytes
    const val: f64 = 82.5;
    const val_bytes: [8]u8 = @bitCast(@as(u64, @bitCast(val)));
    var payload_buf: [128]u8 = undefined;
    const ual_entry = entry_mod.buildCommandEntry(.ts_write, 0, 1, 1, 1000, 0, "cpu", &val_bytes, &payload_buf) orelse unreachable;
    try ts.applyEntry(&ual_entry);

    try testing.expectEqual(@as(u64, 1), ts.stats.points_inserted);
}

test "ts: projection handle vtable" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    const handle = ts.projectionHandle();
    try testing.expect(handle.memoryUsage() > 0);
}
