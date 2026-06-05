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

pub const StoredPoint = struct {
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

/// Series keys are namespace-scoped: a fixed 4-byte little-endian
/// `namespace_hash` prefix, then `measurement\x00field_name`. The prefix may
/// contain 0x00 bytes, so measurement/field parsing always starts at offset
/// NS_PREFIX (never `indexOfScalar` from offset 0).
const NS_PREFIX = 4;

const SeriesKey = struct {
    namespace_hash: u32,
    measurement: []const u8,
    field_name: []const u8,
};

fn seriesKeyHash(key: SeriesKey) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(std.mem.asBytes(&key.namespace_hash));
    hasher.update(key.measurement);
    hasher.update("\x00");
    hasher.update(key.field_name);
    return hasher.final();
}

fn seriesKeyToString(allocator: Allocator, key: SeriesKey) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    var ns_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &ns_bytes, key.namespace_hash, .little);
    try buf.appendSlice(allocator, &ns_bytes);
    try buf.appendSlice(allocator, key.measurement);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, key.field_name);
    return try buf.toOwnedSlice(allocator);
}

/// Namespace hash from a series key's 4-byte prefix.
fn keyNsHash(key: []const u8) u32 {
    if (key.len < NS_PREFIX) return 0;
    return std.mem.readInt(u32, key[0..NS_PREFIX], .little);
}

/// Measurement name from a series key (the bytes after the prefix, up to \x00).
fn keyMeasurement(key: []const u8) []const u8 {
    if (key.len < NS_PREFIX) return key;
    const rest = key[NS_PREFIX..];
    const sep = std.mem.indexOfScalar(u8, rest, 0) orelse rest.len;
    return rest[0..sep];
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
        namespace_hash: u32,
        measurement: []const u8,
        field_name: []const u8,
        value: f64,
        timestamp_ns: u64,
        ual_index: u64,
        tag_hash: u64,
    ) !void {
        const series_str = try seriesKeyToString(self.allocator, .{
            .namespace_hash = namespace_hash,
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
        namespace_hash: u32,
        measurement: []const u8,
        field_name: []const u8,
        min_ts: u64,
        max_ts: u64,
        point_buf: []StoredPoint,
    ) !QueryResult {
        const series_str = try seriesKeyToString(self.allocator, .{
            .namespace_hash = namespace_hash,
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
    pub fn avg(self: *TSProjection, namespace_hash: u32, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(namespace_hash, measurement, field_name, min_ts, max_ts, &buf);
        if (result.points_in_buffer == 0) return null;

        var acc: f64 = 0;
        for (buf[0..result.points_in_buffer]) |p| {
            acc += p.field_value;
        }
        return acc / @as(f64, @floatFromInt(result.points_in_buffer));
    }

    /// Compute the min of points in a range from the write buffer.
    pub fn min(self: *TSProjection, namespace_hash: u32, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(namespace_hash, measurement, field_name, min_ts, max_ts, &buf);
        if (result.points_in_buffer == 0) return null;

        var min_val: f64 = std.math.floatMax(f64);
        for (buf[0..result.points_in_buffer]) |p| {
            min_val = @min(min_val, p.field_value);
        }
        return min_val;
    }

    /// Compute the max of points in a range from the write buffer.
    pub fn max(self: *TSProjection, namespace_hash: u32, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(namespace_hash, measurement, field_name, min_ts, max_ts, &buf);
        if (result.points_in_buffer == 0) return null;

        var max_val: f64 = -std.math.floatMax(f64);
        for (buf[0..result.points_in_buffer]) |p| {
            max_val = @max(max_val, p.field_value);
        }
        return max_val;
    }

    /// Compute the sum of points in a range from the write buffer.
    pub fn sum(self: *TSProjection, namespace_hash: u32, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !?f64 {
        var point_buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(namespace_hash, measurement, field_name, min_ts, max_ts, &point_buf);
        if (result.points_in_buffer == 0) return null;

        var total: f64 = 0;
        for (point_buf[0..result.points_in_buffer]) |p| {
            total += p.field_value;
        }
        return total;
    }

    /// Count points in a range from the write buffer.
    pub fn count(self: *TSProjection, namespace_hash: u32, measurement: []const u8, field_name: []const u8, min_ts: u64, max_ts: u64) !usize {
        var buf: [4096]StoredPoint = undefined;
        const result = try self.queryRange(namespace_hash, measurement, field_name, min_ts, max_ts, &buf);
        return result.points_in_buffer;
    }

    // ─── Info ──────────────────────────────────────────────────────────────

    /// Number of active series (measurement+field combos with data).
    pub fn seriesCount(self: *const TSProjection) usize {
        return self.buffers.count();
    }

    /// Scan unique measurement names into a caller-provided buffer.
    ///
    /// Returns the count of unique names written. Names are borrowed
    /// references into the internal HashMap key storage — valid only
    /// while the projection is not mutated.
    pub fn scanMeasurementNames(self: *const TSProjection, namespace_hash: u32, buf: [][]const u8) usize {
        var result_count: usize = 0;
        var it = self.buffers.iterator();
        while (it.next()) |kv| {
            if (result_count >= buf.len) break;
            const key = kv.key_ptr.*;
            if (keyNsHash(key) != namespace_hash) continue;
            const meas = keyMeasurement(key);
            // Dedup via linear scan (measurement count is typically small)
            var dup = false;
            for (buf[0..result_count]) |existing| {
                if (std.mem.eql(u8, existing, meas)) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                buf[result_count] = meas;
                result_count += 1;
            }
        }
        return result_count;
    }

    /// Return unique measurement names from the write buffers.
    /// Caller owns the returned slices and must free them.
    pub fn listMeasurements(self: *const TSProjection, namespace_hash: u32, allocator: Allocator) ![][]const u8 {
        // Collect unique measurement names from this namespace's buffer keys.
        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();

        var it = self.buffers.iterator();
        while (it.next()) |kv| {
            const key = kv.key_ptr.*;
            if (keyNsHash(key) != namespace_hash) continue;
            _ = try seen.getOrPut(keyMeasurement(key));
        }

        const names = try allocator.alloc([]const u8, seen.count());
        var idx: usize = 0;
        var sit = seen.iterator();
        while (sit.next()) |entry| {
            names[idx] = try allocator.dupe(u8, entry.key_ptr.*);
            idx += 1;
        }
        return names;
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

    // ─── Delete / Retention ────────────────────────────────────────────────

    /// Delete all data for a measurement (all fields).
    /// Returns the number of series removed.
    pub fn deleteMeasurement(self: *TSProjection, namespace_hash: u32, measurement: []const u8) usize {
        var removed: usize = 0;

        // Delete matching write buffers (this namespace + measurement only)
        var bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            const key = kv.key_ptr.*;
            if (keyNsHash(key) == namespace_hash and std.mem.eql(u8, keyMeasurement(key), measurement)) {
                kv.value_ptr.deinit();
                self.allocator.free(@constCast(key));
                self.buffers.removeByPtr(kv.key_ptr);
                removed += 1;
            }
        }

        // Delete matching blocks
        var blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            const key = kv.key_ptr.*;
            if (keyNsHash(key) == namespace_hash and std.mem.eql(u8, keyMeasurement(key), measurement)) {
                kv.value_ptr.deinit(self.allocator);
                self.allocator.free(@constCast(key));
                self.blocks.removeByPtr(kv.key_ptr);
            }
        }

        return removed;
    }

    /// Apply a retention policy: remove all points older than `cutoff_ns`.
    /// Returns the number of points evicted across all series.
    pub fn applyRetention(self: *TSProjection, cutoff_ns: u64) usize {
        var evicted: usize = 0;

        // Evict from write buffers
        var bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            const buf = kv.value_ptr;
            var write_idx: usize = 0;
            for (buf.points.items) |pt| {
                if (pt.timestamp_ns >= cutoff_ns) {
                    buf.points.items[write_idx] = pt;
                    write_idx += 1;
                } else {
                    evicted += 1;
                }
            }
            buf.points.shrinkRetainingCapacity(write_idx);
        }

        // Evict old blocks (entire blocks where max_timestamp < cutoff)
        var blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            const block_list = kv.value_ptr;
            var write_idx: usize = 0;
            for (block_list.items) |block| {
                if (block.max_timestamp_ns >= cutoff_ns) {
                    block_list.items[write_idx] = block;
                    write_idx += 1;
                } else {
                    evicted += block.point_count;
                }
            }
            block_list.shrinkRetainingCapacity(write_idx);
        }

        return evicted;
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
                        cmd.namespace_hash,
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

    /// Clear all state, freeing owned memory.
    pub fn reset(self: *TSProjection) void {
        var bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            kv.value_ptr.deinit();
            self.allocator.free(@constCast(kv.key_ptr.*));
        }
        self.buffers.clearAndFree();

        var blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            kv.value_ptr.deinit(self.allocator);
            self.allocator.free(@constCast(kv.key_ptr.*));
        }
        self.blocks.clearAndFree();

        self.applied_index = 0;
        self.stats = .{};
    }

    // ─── Snapshot Serialization ────────────────────────────────────────────

    /// Serialize the full TS projection state.
    /// Format: [buffer_capacity: u32]
    ///   [buffer_count: u32] then per write buffer:
    ///     [key_len: u16][key bytes][point_count: u32]
    ///     per point: [timestamp_ns: u64][field_value: f64][ual_index: u64][tag_hash: u64]
    ///   [block_series_count: u32] then per block series:
    ///     [key_len: u16][key bytes][block_count: u32]
    ///     per block: [min_ts: u64][max_ts: u64][point_count: u32][ual_start: u64][ual_end: u64]
    /// Caller owns returned slice.
    pub fn serialize(self: *TSProjection, allocator: Allocator) ![]u8 {
        // Calculate total size
        var total_size: usize = 4; // buffer_capacity

        // Buffers: count(4) + per buffer(key_len(2) + key + point_count(4) + points(32 each))
        total_size += 4;
        var bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            total_size += 2 + kv.key_ptr.len + 4 + kv.value_ptr.points.items.len * 32;
        }

        // Blocks: count(4) + per series(key_len(2) + key + block_count(4) + blocks(36 each))
        total_size += 4;
        var blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            total_size += 2 + kv.key_ptr.len + 4 + kv.value_ptr.items.len * 36;
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        // buffer_capacity
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.buffer_capacity), .little);
        offset += 4;

        // Write buffers
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.buffers.count()), .little);
        offset += 4;
        bit = self.buffers.iterator();
        while (bit.next()) |kv| {
            const key = kv.key_ptr.*;
            const wb = kv.value_ptr;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(key.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..key.len], key);
            offset += key.len;
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(wb.points.items.len), .little);
            offset += 4;
            for (wb.points.items) |point| {
                std.mem.writeInt(u64, buf[offset..][0..8], point.timestamp_ns, .little);
                offset += 8;
                @as(*align(1) f64, @ptrCast(buf[offset..][0..8])).* = point.field_value;
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], point.ual_index, .little);
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], point.tag_hash, .little);
                offset += 8;
            }
        }

        // Blocks
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.blocks.count()), .little);
        offset += 4;
        blit = self.blocks.iterator();
        while (blit.next()) |kv| {
            const key = kv.key_ptr.*;
            const block_list = kv.value_ptr;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(key.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..key.len], key);
            offset += key.len;
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(block_list.items.len), .little);
            offset += 4;
            for (block_list.items) |block| {
                std.mem.writeInt(u64, buf[offset..][0..8], block.min_timestamp_ns, .little);
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], block.max_timestamp_ns, .little);
                offset += 8;
                std.mem.writeInt(u32, buf[offset..][0..4], block.point_count, .little);
                offset += 4;
                std.mem.writeInt(u64, buf[offset..][0..8], block.ual_index_start, .little);
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], block.ual_index_end, .little);
                offset += 8;
            }
        }

        return buf;
    }

    /// Restore TS projection state from serialized bytes.
    /// Clears all existing state before restoring.
    pub fn deserialize(self: *TSProjection, data: []const u8) !void {
        self.reset();

        if (data.len < 4) return;
        var offset: usize = 0;

        // buffer_capacity
        self.buffer_capacity = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        // Write buffers
        if (offset + 4 > data.len) return;
        const buf_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var bi: u32 = 0;
        while (bi < buf_count) : (bi += 1) {
            if (offset + 2 > data.len) return;
            const key_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + key_len + 4 > data.len) return;
            const key = try self.allocator.dupe(u8, data[offset..][0..key_len]);
            offset += key_len;
            const point_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            var wb = WriteBuffer.init(self.allocator, self.buffer_capacity);
            errdefer wb.deinit();

            var pi: u32 = 0;
            while (pi < point_count) : (pi += 1) {
                if (offset + 32 > data.len) {
                    wb.deinit();
                    self.allocator.free(key);
                    return;
                }
                const timestamp_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;
                const field_value: f64 = @as(*align(1) const f64, @ptrCast(data[offset..][0..8])).*;
                offset += 8;
                const ual_index = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;
                const tag_hash = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;

                try wb.append(.{
                    .timestamp_ns = timestamp_ns,
                    .field_value = field_value,
                    .ual_index = ual_index,
                    .tag_hash = tag_hash,
                });
            }

            try self.buffers.put(key, wb);
        }

        // Blocks
        if (offset + 4 > data.len) return;
        const block_series_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var si: u32 = 0;
        while (si < block_series_count) : (si += 1) {
            if (offset + 2 > data.len) return;
            const key_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + key_len + 4 > data.len) return;
            const key = try self.allocator.dupe(u8, data[offset..][0..key_len]);
            offset += key_len;
            const block_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            var block_list: std.ArrayList(Block) = .empty;
            errdefer block_list.deinit(self.allocator);

            var bk: u32 = 0;
            while (bk < block_count) : (bk += 1) {
                if (offset + 36 > data.len) {
                    block_list.deinit(self.allocator);
                    self.allocator.free(key);
                    return;
                }
                try block_list.append(self.allocator, .{
                    .min_timestamp_ns = std.mem.readInt(u64, data[offset..][0..8], .little),
                    .max_timestamp_ns = std.mem.readInt(u64, data[offset + 8 ..][0..8], .little),
                    .point_count = std.mem.readInt(u32, data[offset + 16 ..][0..4], .little),
                    .ual_index_start = std.mem.readInt(u64, data[offset + 20 ..][0..8], .little),
                    .ual_index_end = std.mem.readInt(u64, data[offset + 28 ..][0..8], .little),
                });
                offset += 36;
            }

            try self.blocks.put(key, block_list);
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "ts: basic insert and query" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "cpu", "usage", 82.5, 1000, 1, 0);
    try ts.insert(0, "cpu", "usage", 75.0, 2000, 2, 0);
    try ts.insert(0, "cpu", "usage", 90.1, 3000, 3, 0);

    try testing.expectEqual(@as(u64, 3), ts.stats.points_inserted);
    try testing.expectEqual(@as(usize, 1), ts.seriesCount());

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(0, "cpu", "usage", 1000, 3000, &buf);
    try testing.expectEqual(@as(usize, 3), result.points_in_buffer);
    try testing.expectEqual(@as(f64, 82.5), buf[0].field_value);
}

test "ts: range query filters by timestamp" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "mem", "used", 100.0, 1000, 1, 0);
    try ts.insert(0, "mem", "used", 200.0, 2000, 2, 0);
    try ts.insert(0, "mem", "used", 300.0, 3000, 3, 0);
    try ts.insert(0, "mem", "used", 400.0, 4000, 4, 0);

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(0, "mem", "used", 2000, 3000, &buf);
    try testing.expectEqual(@as(usize, 2), result.points_in_buffer);
    try testing.expectEqual(@as(f64, 200.0), buf[0].field_value);
    try testing.expectEqual(@as(f64, 300.0), buf[1].field_value);
}

test "ts: aggregation — avg" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "cpu", "usage", 80.0, 1000, 1, 0);
    try ts.insert(0, "cpu", "usage", 90.0, 2000, 2, 0);
    try ts.insert(0, "cpu", "usage", 100.0, 3000, 3, 0);

    const average = (try ts.avg(0, "cpu", "usage", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 90.0), average, 0.001);
}

test "ts: aggregation — min/max/sum" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "disk", "iops", 10.0, 1000, 1, 0);
    try ts.insert(0, "disk", "iops", 50.0, 2000, 2, 0);
    try ts.insert(0, "disk", "iops", 30.0, 3000, 3, 0);

    const min_val = (try ts.min(0, "disk", "iops", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 10.0), min_val, 0.001);

    const max_val = (try ts.max(0, "disk", "iops", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 50.0), max_val, 0.001);

    const sum_val = (try ts.sum(0, "disk", "iops", 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 90.0), sum_val, 0.001);
}

test "ts: aggregation — count" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "net", "rx", 1.0, 1000, 1, 0);
    try ts.insert(0, "net", "rx", 2.0, 2000, 2, 0);
    try ts.insert(0, "net", "rx", 3.0, 3000, 3, 0);

    const c = try ts.count(0, "net", "rx", 1500, 2500);
    try testing.expectEqual(@as(usize, 1), c); // only ts=2000 in range
}

test "ts: buffer flush creates block" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 3 });
    defer ts.deinit();

    try ts.insert(0, "cpu", "usage", 1.0, 1000, 1, 0);
    try ts.insert(0, "cpu", "usage", 2.0, 2000, 2, 0);
    try testing.expectEqual(@as(u64, 0), ts.stats.blocks_flushed);

    // Third insert triggers flush
    try ts.insert(0, "cpu", "usage", 3.0, 3000, 3, 0);
    try testing.expectEqual(@as(u64, 1), ts.stats.blocks_flushed);
    try testing.expectEqual(@as(usize, 1), ts.totalBlocks());

    // Buffer should be cleared after flush
    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(0, "cpu", "usage", 1000, 3000, &buf);
    try testing.expectEqual(@as(usize, 0), result.points_in_buffer);
    try testing.expectEqual(@as(usize, 1), result.blocks_matched);
}

test "ts: multiple series" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "cpu", "usage", 80.0, 1000, 1, 0);
    try ts.insert(0, "cpu", "idle", 20.0, 1000, 2, 0);
    try ts.insert(0, "mem", "used", 4096.0, 1000, 3, 0);

    try testing.expectEqual(@as(usize, 3), ts.seriesCount());
}

test "ts: query nonexistent series" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(0, "nonexistent", "field", 0, 9999, &buf);
    try testing.expectEqual(@as(usize, 0), result.points_in_buffer);
    try testing.expectEqual(@as(usize, 0), result.blocks_matched);
}

test "ts: memory usage estimate" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts.deinit();

    try ts.insert(0, "cpu", "usage", 80.0, 1000, 1, 0);
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

test "ts: serialize/deserialize round-trip" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 3 });
    defer ts.deinit();

    // Insert points into different series
    try ts.insert(0, "cpu", "usage", 82.5, 1000, 1, 0);
    try ts.insert(0, "cpu", "usage", 75.0, 2000, 2, 0);
    try ts.insert(0, "mem", "used", 4096.0, 1000, 3, 0);

    // Force a flush by filling the buffer (capacity=3)
    try ts.insert(0, "cpu", "usage", 90.0, 3000, 4, 0);
    try testing.expectEqual(@as(u64, 1), ts.stats.blocks_flushed);

    // Serialize
    const data = try ts.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into fresh projection
    var ts2 = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts2.deinit();

    try ts2.deserialize(data);

    // Buffer capacity should be restored from serialized data (3, not 100)
    try testing.expectEqual(@as(usize, 3), ts2.buffer_capacity);

    // Verify series restored
    try testing.expectEqual(@as(usize, 2), ts2.seriesCount()); // cpu\0usage and mem\0used

    // Verify mem\0used buffer has 1 point
    var buf: [10]StoredPoint = undefined;
    const mem_result = try ts2.queryRange(0, "mem", "used", 0, 9999, &buf);
    try testing.expectEqual(@as(usize, 1), mem_result.points_in_buffer);
    try testing.expectApproxEqAbs(@as(f64, 4096.0), buf[0].field_value, 0.001);

    // Verify blocks were restored
    try testing.expectEqual(@as(usize, 1), ts2.totalBlocks());
}

test "ts: serialize empty projection" {
    var ts = TSProjection.init(testing.allocator, .{ .buffer_capacity = 1024 });
    defer ts.deinit();

    const data = try ts.serialize(testing.allocator);
    defer testing.allocator.free(data);

    var ts2 = TSProjection.init(testing.allocator, .{ .buffer_capacity = 100 });
    defer ts2.deinit();

    try ts2.deserialize(data);
    try testing.expectEqual(@as(usize, 1024), ts2.buffer_capacity);
    try testing.expectEqual(@as(usize, 0), ts2.seriesCount());
}
