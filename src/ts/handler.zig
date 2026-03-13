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
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const persistence_mod = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const ReplayRegistry = @import("../storage/persistence.zig").ReplayRegistry;

// FloQL pipeline
const floql_parser = @import("floql/parser.zig");
const floql_executor = @import("floql/executor.zig");
const floql_ast = @import("floql/ast.zig");
const ss_mod = @import("floql/series_set.zig");

const CommandResult = result_mod.CommandResult;
const TSProjection = ts_mod.TSProjection;
const StoredPoint = ts_mod.StoredPoint;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

// ═══════════════════════════════════════════════════════════════════════════════
// TSHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const TSHandler = struct {
    ts: *TSProjection,
    allocator: Allocator,

    /// Monotonic UAL index counter — fallback for test mode (no shard).
    next_ual_index: u64,

    /// Set after init by Shard.wireHandlerShardPtrs(). Required for persistEntry() Raft writes.
    shard_ptr: ?*anyopaque,

    pub fn init(allocator: Allocator, ts: *TSProjection) TSHandler {
        return .{
            .ts = ts,
            .allocator = allocator,
            .next_ual_index = 1,
            .shard_ptr = null,
        };
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.ts_write, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_read, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_query, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_floql, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWalk(.ts_list, dispatchTS, localScanMeasurements);
        dispatcher.registerWithRoute(.ts_delete, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_retention, dispatchTS, preRouteByMeasurement);
    }

    // ── Pre-Route ───────────────────────────────────────────────────────

    fn preRouteByMeasurement(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    // ── Shard Walker: Local Scan ──────────────────────────────────────

    /// ShardWalker LocalScanFn for ts_list — scans unique measurement
    /// names from one shard's TSProjection.
    ///
    /// Returns borrowed references to HashMap key storage (zero allocation).
    /// Safe as long as the projection is not mutated during the walk
    /// (guaranteed — single-threaded shard).
    pub fn localScanMeasurements(
        ctx: *anyopaque,
        _: []const u8,
        _: []const u8,
        _: ?[]const u8,
        _: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const ts: *TSProjection = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [1024][]const u8 = undefined;
        };
        const count = ts.scanMeasurementNames(&S.name_buf);
        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    fn dispatchTS(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.ts_handler.handleCommand(req);
        defer shard.ts_handler.freeResult(result);
        switch (result) {
            .ts_write_ok => shard.namespace_handler.markNamespaceHasData(req.namespace),
            else => {},
        }
        sendTSResponse(shard, conn, req.header.request_id, result);
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

        // Persist through Raft for durability and replication
        var ual_index: u64 = 0;
        if (self.shard_ptr) |sptr| {
            const shard = shardFromPtr(sptr);
            // Encode value as f64 LE bytes for persistence
            var val_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &val_buf, @as(u64, @bitCast(value)), .little);
            ual_index = persistence_mod.persistEntry(shard, .ts_write, entry_mod.Flags.NONE, req.namespace, measurement, &val_buf) catch {
                return .{ .err = .{ .code = .internal_error, .message = "ts write persistence failed" } };
            };
        } else {
            ual_index = self.nextUalIndex();
        }

        // Insert into local TS projection (full fidelity: field_name + tag_hash)
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
        // FloQL query string is carried in req.value (key is empty).
        const query_str = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "floql query string is required" } };
        };

        // 1. Parse the FloQL query
        var query = floql_parser.Parser.parse(query_str, self.allocator) catch {
            return .{ .err = .{ .code = .invalid_request, .message = "floql parse error" } };
        };
        defer query.deinit(self.allocator);

        // 2. Resolve source: query.source → measurement + time range → StoredPoint[]
        const measurement = query.source.measurement;
        if (measurement.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "floql: measurement name is required" } };
        }

        // Resolve time range
        const now_ms: i64 = std.time.milliTimestamp();
        var from_ns: u64 = 0;
        var to_ns: u64 = std.math.maxInt(u64);
        if (query.source.range.duration_ms > 0) {
            const from_ms = now_ms - query.source.range.duration_ms;
            from_ns = if (from_ms > 0) @intCast(@as(u64, @bitCast(from_ms)) * 1_000_000) else 0;
            to_ns = @intCast(@as(u64, @bitCast(now_ms)) * 1_000_000);
        } else if (query.source.range.from_ms > 0) {
            from_ns = @intCast(@as(u64, @bitCast(query.source.range.from_ms)) * 1_000_000);
            if (query.source.range.to_ms > 0) {
                to_ns = @intCast(@as(u64, @bitCast(query.source.range.to_ms)) * 1_000_000);
            }
        }

        // Resolve field: from field() stage (if present), default to "value"
        var field_name: []const u8 = "value";
        for (query.stages) |stage| {
            switch (stage) {
                .field => |f| {
                    field_name = f.name;
                    break;
                },
                else => {},
            }
        }

        // 3. Query the TSProjection for raw points
        var point_buf: [4096]StoredPoint = undefined;
        const qr = self.ts.queryRange(measurement, field_name, from_ns, to_ns, &point_buf) catch {
            return .{ .err = .{ .code = .internal_error, .message = "floql: ts query failed" } };
        };

        // 4. Convert StoredPoint[] → SeriesSet (initial pipeline input)
        const count_pts = qr.points_in_buffer;
        var dp_slice = self.allocator.alloc(ss_mod.DataPoint, count_pts) catch {
            return .{ .err = .{ .code = .internal_error, .message = "floql: out of memory" } };
        };
        for (point_buf[0..count_pts], 0..) |sp, i| {
            dp_slice[i] = .{
                .timestamp_ms = @intCast(sp.timestamp_ns / 1_000_000),
                .value = sp.field_value,
            };
        }

        var series_slice = self.allocator.alloc(ss_mod.Series, 1) catch {
            self.allocator.free(dp_slice);
            return .{ .err = .{ .code = .internal_error, .message = "floql: out of memory" } };
        };
        series_slice[0] = .{
            .key = measurement,
            .field = field_name,
            .points = dp_slice,
        };

        var initial = ss_mod.SeriesSet.fromOwned(self.allocator, series_slice);

        // 5. Execute the pipeline stages
        // Note: execute() returns the input unchanged (same pointer) when pipeline is empty.
        // When pipeline has stages, it returns a new SeriesSet and does NOT free initial.
        var result_set = floql_executor.execute(query.stages, initial, self.allocator) catch {
            initial.deinit();
            return .{ .err = .{ .code = .internal_error, .message = "floql: execution failed" } };
        };
        defer result_set.deinit();

        // If pipeline had stages, initial is a separate allocation — free it
        if (query.stages.len > 0) {
            initial.deinit();
        }

        // 6. Encode SeriesSet → wire bytes
        const encoded = result_set.encode(self.allocator) catch {
            return .{ .err = .{ .code = .internal_error, .message = "floql: encoding failed" } };
        };

        return .{ .ts_floql_result = .{ .data = encoded } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *TSHandler, req: Request) CommandResult {
        _ = req;

        const names = self.ts.listMeasurements(self.allocator) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts list failed" } };
        };
        defer {
            for (names) |n| self.allocator.free(n);
            self.allocator.free(names);
        }

        const data = serializeListResult(self.allocator, names) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts list serialization failed" } };
        };

        return .{ .ts_list_result = .{ .data = data } };
    }

    // ── DELETE ───────────────────────────────────────────────────────────

    fn handleDelete(self: *TSHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "measurement name is required" } };
        }

        const removed = self.ts.deleteMeasurement(req.key);
        _ = removed;
        return .ok;
    }

    // ── RETENTION ───────────────────────────────────────────────────────

    fn handleRetention(self: *TSHandler, req: Request) CommandResult {
        // Retention policy: key = measurement, duration from TLV option or value
        // Client sends raw_ttl via OptionTag.ts_raw_ttl; fallback to req.value
        const duration_str: []const u8 = if (req.findOption(.ts_raw_ttl)) |opt|
            opt.asString()
        else if (req.value.len > 0)
            req.value
        else {
            return .{ .err = .{ .code = .invalid_request, .message = "retention duration is required (e.g. '7d', '24h')" } };
        };

        const duration_ms = floql_ast.parseDuration(duration_str) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "invalid retention duration" } };
        };

        const now_ms = std.time.milliTimestamp();
        const cutoff_ms = now_ms - duration_ms;
        const cutoff_ns: u64 = if (cutoff_ms > 0)
            @intCast(@as(u64, @bitCast(cutoff_ms)) * 1_000_000)
        else
            0;

        const evicted = self.ts.applyRetention(cutoff_ns);
        _ = evicted;
        return .ok;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    pub fn nextUalIndex(self: *TSHandler) u64 {
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
            .ts_floql_result => |r| self.allocator.free(r.data),
            else => {},
        }
    }

    // ── Replay Registration ─────────────────────────────────────────────

    /// Register TS entry types with the ReplayRegistry so persisted entries
    /// are replayed back to the TS projection on startup.
    pub fn registerReplay(self: *TSHandler, registry: *ReplayRegistry) void {
        registry.register(.ts_write, self, replayEntry);
        registry.register(.ts_write_batch, self, replayEntry);
    }

    /// Replay callback — rebuild TS projection from persisted UAL entries.
    fn replayEntry(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *TSHandler = @ptrCast(@alignCast(ctx));
        if (entry_mod.CommandPayload.deserialize(entry.payload)) |cmd| {
            var value: f64 = 0.0;
            if (cmd.value.len >= 8) {
                value = @bitCast(std.mem.readInt(u64, cmd.value[0..8], .little));
            }
            self.ts.insert(
                cmd.key,
                "value",
                value,
                entry.header.timestamp_ns,
                entry.header.index,
                0,
            ) catch {};
        }
    }

    /// Cast opaque shard pointer to Shard for persistEntry().
    fn shardFromPtr(ptr: *anyopaque) *Shard {
        return @ptrCast(@alignCast(ptr));
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Parsing Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn parseF64FromString(s: []const u8) ?f64 {
    return std.fmt.parseFloat(f64, s) catch null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Response Dispatch
// ═══════════════════════════════════════════════════════════════════════════════

fn sendTSResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            shard.sendErrorResponse(conn, request_id, errorCodeToStatus(e.code), e.message);
        },
        .ts_write_ok => |w| {
            // Send [series_hash:u64][timestamp_ms:i64][sequence:u64] (24 bytes)
            var buf: [24]u8 = undefined;
            std.mem.writeInt(u64, buf[0..8], w.series_hash, .little);
            std.mem.writeInt(i64, buf[8..16], w.timestamp_ms, .little);
            std.mem.writeInt(u64, buf[16..24], w.sequence, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        },
        .ts_read_result => |r| {
            shard.sendOkResponse(conn, request_id, r.data);
        },
        .ts_query_result => |r| {
            shard.sendOkResponse(conn, request_id, r.data);
        },
        .ts_list_result => |r| {
            shard.sendOkResponse(conn, request_id, r.data);
        },
        .ts_floql_result => |r| {
            shard.sendOkResponse(conn, request_id, r.data);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled ts response");
        },
    }
}

fn errorCodeToStatus(code: CommandResult.ErrorCode) proto.StatusCode {
    return switch (code) {
        .invalid_request => .bad_request,
        .unauthorized => .unauthorized,
        .not_found => .not_found,
        .already_exists => .conflict,
        .timeout => .internal_error,
        .internal_error => .internal_error,
        .unavailable => .internal_error,
        .conflict => .conflict,
        else => .internal_error,
    };
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
/// Wire format: [count:u32]([name_len:u16][name])*[has_more:u8][cursor_len:u16]
fn serializeListResult(allocator: Allocator, names: []const []const u8) ![]u8 {
    // Calculate total size
    var total: usize = 4; // count
    for (names) |n| {
        total += 2 + n.len; // name_len + name
    }
    total += 1; // has_more
    total += 2; // cursor_len (0)

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], @intCast(names.len), .little);
    var pos: usize = 4;
    for (names) |n| {
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(n.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..n.len], n);
        pos += n.len;
    }
    buf[pos] = 0; // has_more = false
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 0, .little); // cursor_len = 0
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

    // Same measurement, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.ts_write, "cpu", "", "");
    req_ns.namespace = "other";
    try testing.expect(TSHandler.preRouteByMeasurement(req1) != TSHandler.preRouteByMeasurement(req_ns));
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

test "ts handler: floql basic pipeline" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // Write data points
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "10.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "20.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "30.0", ""));

    // Execute FloQL query: get all points from cpu in a large time range
    const result = handler.handleCommand(makeRequest(.ts_floql, "", "cpu[24h]", ""));
    switch (result) {
        .ts_floql_result => |r| {
            defer handler.freeResult(result);
            // Should have encoded SeriesSet with 1 series, 3 points
            const series_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), series_count);
        },
        .err => |e| {
            std.debug.print("FloQL error: {s}\n", .{e.message});
            return error.TestUnexpectedResult;
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: floql empty query" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_floql, "", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: floql invalid syntax" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_floql, "", "|||bad", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: floql with pipeline stages" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // Write some data
    for (0..6) |i| {
        var val_buf: [32]u8 = undefined;
        const val_str = std.fmt.bufPrint(&val_buf, "{d}.0", .{(i + 1) * 10}) catch unreachable;
        _ = handler.handleCommand(makeRequest(.ts_write, "mem", val_str, ""));
    }

    // Execute: mem[24h] | where(> 30)
    const result = handler.handleCommand(makeRequest(.ts_floql, "", "mem[24h] | where(> 30)", ""));
    switch (result) {
        .ts_floql_result => |r| {
            defer handler.freeResult(result);
            const series_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), series_count);
        },
        .err => |e| {
            std.debug.print("FloQL pipeline error: {s}\n", .{e.message});
            return error.TestUnexpectedResult;
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: delete measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "10.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", "20.0", ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "memory", "30.0", ""));

    try testing.expectEqual(@as(u64, 3), ts.stats.points_inserted);

    // Delete cpu measurement
    const del_result = handler.handleCommand(makeRequest(.ts_delete, "cpu", "", ""));
    switch (del_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // cpu should be gone
    const read_result = handler.handleCommand(makeRequest(.ts_read, "cpu", "", ""));
    switch (read_result) {
        .ts_read_result => |r| {
            defer handler.freeResult(read_result);
            const count_pts = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count_pts);
        },
        else => return error.TestUnexpectedResult,
    }

    // memory should still exist
    const mem_result = handler.handleCommand(makeRequest(.ts_read, "memory", "", ""));
    switch (mem_result) {
        .ts_read_result => |r| {
            defer handler.freeResult(mem_result);
            const count_pts = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), count_pts);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: delete empty measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_delete, "", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: retention empty duration" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_retention, "", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: retention invalid duration" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator, .{});
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_retention, "", "bad", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}
