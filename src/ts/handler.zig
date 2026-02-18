//! TimeSeries Handler — registers TS opcodes with Dispatcher and handles time-series operations.
//!
//! Read operations (ts_read, ts_query, ts_list) query the TSProjection directly.
//! Write operations (ts_write) go through the TSProjection directly for now;
//! they will be rewired through Raft propose when the full pipeline is connected.
//!
//! ## Opcode Range
//!
//!   Commands:   0xE0–0xE6  (write, read, query, floql, list, delete, retention)
//!   Responses:  0xE7–0xED
//!
//! ## TS Semantics
//!
//! - Each data point has a measurement name, field name, value, and timestamp.
//! - Writes append to a per-series write buffer; buffers flush to immutable blocks.
//! - Reads return raw data points within a time range.
//! - Queries return aggregated values (avg, sum, count, min, max) over time windows.
//! - FloQL provides a pipeline query language (KEEP — parser + executor).

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const ts_mod = @import("../projection/ts.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");

const CommandResult = result_mod.CommandResult;
const TSProjection = ts_mod.TSProjection;
const StoredPoint = ts_mod.StoredPoint;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

// ═══════════════════════════════════════════════════════════════════════════════
// TSHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const TSHandler = struct {
    ts: *TSProjection,
    allocator: Allocator,

    /// Monotonic UAL index counter — stand-in for real UAL index.
    next_ual_index: u64,

    pub fn init(allocator: Allocator, ts: *TSProjection) TSHandler {
        return .{
            .ts = ts,
            .allocator = allocator,
            .next_ual_index = 1,
        };
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.ts_write, dispatchStub, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_read, dispatchStub, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_query, dispatchStub, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_floql, dispatchStub, preRouteByMeasurement);
        dispatcher.register(.ts_list, dispatchStub);
        dispatcher.registerWithRoute(.ts_delete, dispatchStub, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_retention, dispatchStub, preRouteByMeasurement);
    }

    // ── Pre-Route ───────────────────────────────────────────────────────

    fn preRouteByMeasurement(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return std.hash.Wyhash.hash(0, req.key);
    }

    fn dispatchStub(shard: *anyopaque, conn: *anyopaque, req: Request) void {
        _ = shard;
        _ = conn;
        _ = req;
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *TSHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .ts_write => self.handleWrite(req),
            .ts_read => self.handleRead(req),
            .ts_query => self.handleQuery(req),
            .ts_floql => self.handleFloQL(req),
            .ts_list => self.handleList(req),
            .ts_delete => self.handleDelete(req),
            .ts_retention => self.handleRetention(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown ts opcode" } },
        };
    }

    // ── WRITE ───────────────────────────────────────────────────────────

    fn handleWrite(self: *TSHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "measurement name is required" } };
        }

        const measurement = req.key;
        const ual_index = self.nextUalIndex();

        // Parse field name from options, default to "value"
        const field_name = if (req.findOption(.ts_field)) |opt|
            opt.asString()
        else
            "value";

        // Parse value from payload
        const value: f64 = if (req.value.len >= 8)
            @bitCast(std.mem.readInt(u64, req.value[0..8], .little))
        else if (req.value.len > 0)
            parseF64FromString(req.value) orelse {
                return .{ .err = .{ .code = .invalid_request, .message = "invalid value" } };
            }
        else
            0.0;

        // Timestamp: from options, or server-generated
        const timestamp_ns: u64 = if (req.findOption(.ts_timestamp)) |opt| blk: {
            const ts_ms = opt.asI64() orelse 0;
            break :blk if (ts_ms > 0) @intCast(@as(u64, @bitCast(ts_ms)) * 1_000_000) else serverTimestampNs();
        } else serverTimestampNs();

        // Tag hash (simplified): hash the comma-separated tag string if present
        const tag_hash: u64 = if (req.findOption(.ts_tags)) |opt| blk: {
            const tags_str = opt.asString();
            break :blk if (tags_str.len > 0) std.hash.Wyhash.hash(0, tags_str) else 0;
        } else 0;

        self.ts.insert(measurement, field_name, value, timestamp_ns, ual_index, tag_hash) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts write failed" } };
        };

        const timestamp_ms: i64 = @intCast(timestamp_ns / 1_000_000);

        return .{ .ts_write_ok = .{
            .series_hash = std.hash.Wyhash.hash(0, measurement),
            .timestamp_ms = timestamp_ms,
            .sequence = ual_index,
        } };
    }

    // ── READ ────────────────────────────────────────────────────────────

    fn handleRead(self: *TSHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "measurement name is required" } };
        }

        const measurement = req.key;

        const field_name = if (req.findOption(.ts_field)) |opt|
            opt.asString()
        else
            "value";

        const from_ns = if (req.findOption(.ts_from_ms)) |opt| blk: {
            const ms = opt.asI64() orelse 0;
            break :blk if (ms > 0) @as(u64, @bitCast(ms)) * 1_000_000 else 0;
        } else 0;

        const to_ns = if (req.findOption(.ts_to_ms)) |opt| blk: {
            const ms = opt.asI64() orelse 0;
            break :blk if (ms > 0) @as(u64, @bitCast(ms)) * 1_000_000 else std.math.maxInt(u64);
        } else std.math.maxInt(u64);

        // Query raw points
        var point_buf: [4096]StoredPoint = undefined;
        const result = self.ts.queryRange(measurement, field_name, from_ns, to_ns, &point_buf) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts read failed" } };
        };

        const count_pts = result.points_in_buffer;
        const data = serializeDataPoints(self.allocator, point_buf[0..count_pts]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts read serialization failed" } };
        };

        return .{ .ts_read_result = .{ .data = data } };
    }

    // ── QUERY (aggregation) ─────────────────────────────────────────────

    fn handleQuery(self: *TSHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "measurement name is required" } };
        }

        const measurement = req.key;

        const field_name = if (req.findOption(.ts_field)) |opt|
            opt.asString()
        else
            "value";

        const from_ns = if (req.findOption(.ts_from_ms)) |opt| blk: {
            const ms = opt.asI64() orelse 0;
            break :blk if (ms > 0) @as(u64, @bitCast(ms)) * 1_000_000 else 0;
        } else 0;

        const to_ns = if (req.findOption(.ts_to_ms)) |opt| blk: {
            const ms = opt.asI64() orelse 0;
            break :blk if (ms > 0) @as(u64, @bitCast(ms)) * 1_000_000 else std.math.maxInt(u64);
        } else std.math.maxInt(u64);

        const agg_name = if (req.findOption(.ts_aggregation)) |opt|
            opt.asString()
        else
            "avg";

        // Dispatch to the appropriate aggregation function
        const agg_value: ?f64 = if (std.mem.eql(u8, agg_name, "avg"))
            self.ts.avg(measurement, field_name, from_ns, to_ns) catch null
        else if (std.mem.eql(u8, agg_name, "sum"))
            self.ts.sum(measurement, field_name, from_ns, to_ns) catch null
        else if (std.mem.eql(u8, agg_name, "min"))
            self.ts.min(measurement, field_name, from_ns, to_ns) catch null
        else if (std.mem.eql(u8, agg_name, "max"))
            self.ts.max(measurement, field_name, from_ns, to_ns) catch null
        else if (std.mem.eql(u8, agg_name, "count")) blk: {
            const c = self.ts.count(measurement, field_name, from_ns, to_ns) catch 0;
            break :blk @as(f64, @floatFromInt(c));
        } else null;

        const data = serializeAggResult(self.allocator, measurement, agg_value) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts query serialization failed" } };
        };

        return .{ .ts_query_result = .{ .data = data } };
    }

    // ── FLOQL ───────────────────────────────────────────────────────────

    fn handleFloQL(self: *TSHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // FloQL execution is wired through the existing parser + executor (KEEP files).
        // Full integration requires cross-module wiring — stub for now.
        return .{ .err = .{ .code = .invalid_request, .message = "floql not yet wired" } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *TSHandler, req: Request) CommandResult {
        _ = req;

        const series_count = self.ts.seriesCount();
        const data = serializeListResult(self.allocator, series_count) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts list serialization failed" } };
        };

        return .{ .ts_list_result = .{ .data = data } };
    }

    // ── DELETE ───────────────────────────────────────────────────────────

    fn handleDelete(self: *TSHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // Delete requires removing series from the projection — not yet in API
        return .{ .err = .{ .code = .invalid_request, .message = "ts delete not yet implemented" } };
    }

    // ── RETENTION ───────────────────────────────────────────────────────

    fn handleRetention(self: *TSHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // Retention policy management — not yet implemented
        return .{ .err = .{ .code = .invalid_request, .message = "ts retention not yet implemented" } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextUalIndex(self: *TSHandler) u64 {
        const idx = self.next_ual_index;
        self.next_ual_index += 1;
        return idx;
    }

    fn serverTimestampNs() u64 {
        return @intCast(@as(u64, @bitCast(@as(i64, std.time.milliTimestamp()))) * 1_000_000);
    }

    pub fn freeResult(self: *TSHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .ts_read_result => |r| self.allocator.free(r.data),
            .ts_query_result => |r| self.allocator.free(r.data),
            .ts_list_result => |r| self.allocator.free(r.data),
            else => {},
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Parsing Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn parseF64FromString(s: []const u8) ?f64 {
    return std.fmt.parseFloat(f64, s) catch null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization
// ═══════════════════════════════════════════════════════════════════════════════

/// Serialize raw data points.
/// Wire format: [count:u32] per point: [timestamp_ms:i64 LE][value:f64 LE]
fn serializeDataPoints(allocator: Allocator, points: []const StoredPoint) ![]u8 {
    const entry_size: usize = 16; // i64 + f64
    const total = 4 + points.len * entry_size;

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);
    var offset: usize = 0;

    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(points.len), .little);
    offset += 4;

    for (points) |pt| {
        const ts_ms: i64 = @intCast(pt.timestamp_ns / 1_000_000);
        std.mem.writeInt(i64, buf[offset..][0..8], ts_ms, .little);
        offset += 8;
        const val_bits: u64 = @bitCast(pt.field_value);
        std.mem.writeInt(u64, buf[offset..][0..8], val_bits, .little);
        offset += 8;
    }

    return buf;
}

/// Serialize aggregation result.
/// Wire format: [series_count:u32][key_len:u32][key][bucket_count:u32][window_start_ms:i64][value:f64]
fn serializeAggResult(allocator: Allocator, measurement: []const u8, value: ?f64) ![]u8 {
    if (value) |v| {
        // 1 series, 1 bucket
        const total = 4 + 4 + measurement.len + 4 + 8 + 8;
        const buf = try allocator.alloc(u8, total);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], 1, .little); // series_count
        offset += 4;
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(measurement.len), .little); // key_len
        offset += 4;
        @memcpy(buf[offset .. offset + measurement.len], measurement);
        offset += measurement.len;
        std.mem.writeInt(u32, buf[offset..][0..4], 1, .little); // bucket_count
        offset += 4;
        std.mem.writeInt(i64, buf[offset..][0..8], 0, .little); // window_start_ms
        offset += 8;
        const val_bits: u64 = @bitCast(v);
        std.mem.writeInt(u64, buf[offset..][0..8], val_bits, .little);

        return buf;
    } else {
        // 0 series
        const buf = try allocator.alloc(u8, 4);
        errdefer allocator.free(buf);
        std.mem.writeInt(u32, buf[0..4], 0, .little);
        return buf;
    }
}

/// Serialize list result.
/// Wire format: [count:u32]
fn serializeListResult(allocator: Allocator, series_count: usize) ![]u8 {
    const buf = try allocator.alloc(u8, 4);
    errdefer allocator.free(buf);
    std.mem.writeInt(u32, buf[0..4], @intCast(series_count), .little);
    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;
const OptionsBuilder = proto.OptionsBuilder;

fn makeRequest(op: OpCode, key: []const u8, value: []const u8, options: []const u8) Request {
    return .{
        .header = .{
            .magic = proto.MAGIC,
            .payload_length = 0,
            .request_id = 1,
            .crc32 = 0,
            .version = proto.VERSION,
            .op_code = @intFromEnum(op),
            .flags = 0,
            .reserved = 0,
        },
        .namespace = "default",
        .key = key,
        .value = value,
        .options = options,
    };
}

test "ts handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    TSHandler.register(&dispatcher);

    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_write)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_read)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_query)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_floql)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_list)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.ts_retention)] != null);

    try testing.expectEqual(@as(u16, 7), dispatcher.handler_count);
}

test "ts handler: write with string value" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_write, "cpu", "42.5", ""));
    switch (result) {
        .ts_write_ok => |w| {
            try testing.expect(w.series_hash != 0);
            try testing.expect(w.timestamp_ms > 0);
            try testing.expectEqual(@as(u64, 1), w.sequence);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u64, 1), ts.stats.points_inserted);
}

test "ts handler: write with binary f64 value" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // Encode 99.9 as LE f64 bytes
    var val_buf: [8]u8 = undefined;
    const val_bits: u64 = @bitCast(@as(f64, 99.9));
    std.mem.writeInt(u64, &val_buf, val_bits, .little);

    const result = handler.handleCommand(makeRequest(.ts_write, "memory", &val_buf, ""));
    switch (result) {
        .ts_write_ok => |w| {
            try testing.expectEqual(@as(u64, 1), w.sequence);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: write with field name option" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    var opts_buf: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addString(.ts_field, "usage_percent");
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.ts_write, "cpu", "82.5", opts));
    switch (result) {
        .ts_write_ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: write empty measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_write, "", "42.0", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: read" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // Write some data points
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "10.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "20.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "30.0", ""));

    // Read all
    const result = handler.handleCommand(makeRequest(.ts_read, "cpu", "", ""));
    switch (result) {
        .ts_read_result => |r| {
            defer handler.freeResult(result);
            const count_pts = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count_pts);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: read empty measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_read, "", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: read non-existent series" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_read, "nonexistent", "", ""));
    switch (result) {
        .ts_read_result => |r| {
            defer handler.freeResult(result);
            const count_pts = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count_pts);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: query avg" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "disk", "10.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "disk", "20.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "disk", "30.0", ""));

    // Query avg
    var opts_buf: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addString(.ts_aggregation, "avg");
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.ts_query, "disk", "", opts));
    switch (result) {
        .ts_query_result => |r| {
            defer handler.freeResult(result);
            const series_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), series_count);
            // There should be a result with measurement key and value
            try testing.expect(r.data.len > 4);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: query count" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "net", "1.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "net", "2.0", ""));

    var opts_buf: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addString(.ts_aggregation, "count");
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.ts_query, "net", "", opts));
    switch (result) {
        .ts_query_result => |r| {
            defer handler.freeResult(result);
            // Should have 1 series with count = 2.0
            const series_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), series_count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: query non-existent series" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_query, "nope", "", ""));
    switch (result) {
        .ts_query_result => |r| {
            defer handler.freeResult(result);
            const series_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), series_count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: list" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "1.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "memory", "2.0", ""));

    const result = handler.handleCommand(makeRequest(.ts_list, "", "", ""));
    switch (result) {
        .ts_list_result => |r| {
            defer handler.freeResult(result);
            const count_series = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 2), count_series);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: pre-route by measurement" {
    const req1 = makeRequest(.ts_write, "cpu", "", "");
    const req2 = makeRequest(.ts_write, "cpu", "", "");
    const req3 = makeRequest(.ts_write, "memory", "", "");

    try testing.expectEqual(TSHandler.preRouteByMeasurement(req1), TSHandler.preRouteByMeasurement(req2));
    try testing.expect(TSHandler.preRouteByMeasurement(req1) != TSHandler.preRouteByMeasurement(req3));

    const req_empty = makeRequest(.ts_write, "", "", "");
    try testing.expectEqual(@as(?u64, 0), TSHandler.preRouteByMeasurement(req_empty));
}

test "ts handler: multiple writes same measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    for (0..10) |i| {
        var val_buf: [32]u8 = undefined;
        const val_str = std.fmt.bufPrint(&val_buf, "{d}.0", .{i}) catch unreachable;
        const result = handler.handleCommand(makeRequest(.ts_write, "temp", val_str, ""));
        switch (result) {
            .ts_write_ok => {},
            else => return error.TestUnexpectedResult,
        }
    }

    try testing.expectEqual(@as(u64, 10), ts.stats.points_inserted);
    try testing.expectEqual(@as(u64, 10), handler.next_ual_index - 1);
}

test "ts handler: freeResult non-heap-allocated is no-op" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // ok result should be safe to free (no-op)
    handler.freeResult(.ok);
    handler.freeResult(.{ .err = .{ .code = .invalid_request, .message = "test" } });
}
