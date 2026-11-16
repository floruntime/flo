//! TimeSeries Handler — registers TS opcodes with Dispatcher and handles time-series operations.
//!
//! Read operations (ts.read, ts_query, ts_list) query the TSProjection directly.
//! Write operations (ts_write) persist an entry through Raft; the projection
//! router inserts the point under `Shard.applyCommitted()`.
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
const time_units = @import("../util/time_units.zig");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const ts_mod = @import("../projection/ts.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Result = persistence_mod.Result;
const entry_mod = @import("../storage/ual/entry.zig");

// FloQL pipeline
const floql_parser = @import("floql/parser.zig");
const floql_executor = @import("floql/executor.zig");
const floql_ast = @import("floql/ast.zig");
const ss_mod = @import("floql/series_set.zig");

const CommandResult = result_mod.CommandResult;
const TSProjection = ts_mod.TSProjection;

/// Most points a read, or a FloQL answer, returns.
pub const MAX_READ_POINTS = ts_ops.MAX_READ;
pub const TOO_MANY_FLOQL_POINTS = std.fmt.comptimePrint("bad request: the answer holds more than {d} points; aggregate them, narrow the query, or page through them with ts read", .{MAX_READ_POINTS});
/// Most points a FloQL query reads to run its stages over: it holds them
/// all at once, 16 bytes each, on the shard thread.
pub const MAX_FLOQL_POINTS = 1 << 17;
/// Most points `ts query` reads into its buckets, so one query's time on
/// the shard thread has a bound.
pub const MAX_QUERY_SCANNED = 1 << 20;
pub const TOO_MANY_FLOQL_SCANNED = std.fmt.comptimePrint("bad request: more than {d} points match a FloQL query; narrow its range or its tags", .{MAX_FLOQL_POINTS});
pub const TOO_MANY_QUERY_POINTS = std.fmt.comptimePrint("bad request: more than {d} points match a query; narrow its range or its tags", .{MAX_QUERY_SCANNED});
pub const TOO_MANY_QUERY_BUCKETS = std.fmt.comptimePrint("bad request: the range holds more than {d} windows of this width; widen the window (it is in nanoseconds) or narrow the range", .{MAX_QUERY_BUCKETS});
const StoredPoint = ts_mod.StoredPoint;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const table_run = @import("../node/table_run.zig");
const schema = @import("../protocol/schema.zig");
const ts_ops = @import("../protocol/ts_ops.zig");
const InfoResponse = table_run.op("ts.info").response;
const ReadResponse = table_run.op("ts.read").response;
pub const ReadOutcome = table_run.Outcome(ReadResponse);

// ═══════════════════════════════════════════════════════════════════════════════
// TSHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const TSHandler = struct {
    ts: *TSProjection,
    allocator: Allocator,

    /// Monotonic UAL index counter — fallback for test mode (no shard).
    next_ual_index: u64,

    /// Set after init by Shard.wireHandlerShardPtrs(). Required to propose Raft entries.
    shard_ptr: ?*anyopaque,

    /// Holds a refusal that names the request's own input. The next
    /// request overwrites it, so a refusal from it is sent at once, never
    /// parked.
    why_buf: [512]u8 = undefined,
    /// A read's page and its cursor, encoded; written before it is sent.
    read_buf: [ts_ops.MAX_READ * 16]u8 = undefined,
    cursor_buf: [ts_ops.CURSOR_LEN]u8 = undefined,
    /// An info answer's field names, encoded; written before it is sent.
    info_buf: [ts_ops.MAX_INFO_FIELD_BYTES]u8 = undefined,
    /// How many points a query and a FloQL query may read; lowered only
    /// by tests.
    query_scanned_max: usize = MAX_QUERY_SCANNED,
    floql_points_max: usize = MAX_FLOQL_POINTS,

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
        dispatcher.registerWithRoute(.ts_read, table_run.handler("ts.read", TSHandler, runRead), preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_query, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_floql, dispatchTS, preRouteByQueryMeasurement);
        dispatcher.registerWalk(.ts_list, dispatchTS, localScanMeasurements);
        dispatcher.setWalkEntries(.ts_list, writeListEntries);
        dispatcher.registerWithRoute(.ts_delete, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_retention, dispatchTS, preRouteByMeasurement);
        dispatcher.registerWithRoute(.ts_info, table_run.handler("ts.info", TSHandler, runInfo), preRouteByMeasurement);
    }

    // ── Info ────────────────────────────────────────────────────────────

    pub const Error = error{};

    pub fn refusal(err: Error) table_run.Refused {
        switch (err) {}
    }

    /// A measurement's summary from its series keys and block headers. One
    /// with no series here is not found.
    fn runInfo(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(InfoResponse) {
        const self = shard.ts_handler;
        var names: [TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
        const sum = self.ts.summarize(req.ns_id, req.key, &names);
        if (sum.series == 0) return .{ .refused = .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = "not found: no such measurement" } } };
        // Listed while they fit; the rest are counted, not named.
        var w: schema.Writer = .{ .buf = &self.info_buf };
        var listed: u32 = 0;
        for (names[0..sum.fields]) |n| {
            if (w.pos + 2 + n.len > ts_ops.MAX_INFO_FIELD_BYTES) break;
            ts_ops.FieldName.encode(&w, n) catch break;
            listed += 1;
        }
        const has_points = sum.points > 0;
        return .{ .answer = .{
            .series = sum.series,
            .field_count = sum.fields,
            .fields_capped = sum.fields_capped or listed < sum.fields,
            .fields = .{ .bytes = self.info_buf[0..w.pos], .count = listed },
            .points = sum.points,
            .oldest_ns = if (has_points) @intCast(sum.oldest_ns) else null,
            .newest_ns = if (has_points) @intCast(sum.newest_ns) else null,
        } };
    }

    fn runRead(shard: *Shard, _: *Connection, req: Request, field: []const u8, tags: []const u8, from_ns: ?i64, to_ns: ?i64, limit: u32, order: ts_ops.Order, cursor: []const u8) Error!ReadOutcome {
        return shard.ts_handler.readPage(req.ns_id, req.key, field, tags, from_ns, to_ns, limit, order, cursor);
    }

    fn badRead(message: []const u8) ReadOutcome {
        return .{ .refused = .{ .status = .bad_request, .refusal = .{ .reason = .malformed, .ran = .no, .message = message } } };
    }

    /// One page of a measurement field's points in `[from_ns, to_ns]`, in
    /// `order`, past `cursor` (the last page's) when it isn't empty. A page
    /// that stops short of the last matching point answers a cursor to go
    /// on from; a read never refuses for how many points match.
    pub fn readPage(self: *TSHandler, ns: router.NamespaceId, measurement: []const u8, field: []const u8, tags: []const u8, from_ns: ?i64, to_ns: ?i64, limit: u32, order: ts_ops.Order, cursor: []const u8) ReadOutcome {
        if (measurement.len == 0) return badRead("bad request: measurement name is required");
        var canon_buf: [ts_mod.MAX_TAG_STRING]u8 = undefined;
        var preds_buf: [ts_mod.MAX_TAGS]ts_mod.TagPredicate = undefined;
        const filter = switch (ts_mod.parseTagFilter(tags, &canon_buf, &preds_buf)) {
            .ok => |f| f,
            .not_pair => |part| return badRead(std.fmt.bufPrint(&self.why_buf, "bad request: tag filter part '{s}' is not key=value", .{part}) catch "bad request: a tag filter part is not key=value"),
            .too_many => return badRead(std.fmt.comptimePrint("bad request: a tag filter takes at most {d} tags", .{ts_mod.MAX_TAGS})),
            .too_long => return badRead(std.fmt.comptimePrint("bad request: a tag filter is at most {d} bytes", .{ts_mod.MAX_TAG_STRING})),
        };
        const after: ?ts_mod.TSProjection.ReadAt = switch (cursor.len) {
            0 => null,
            ts_ops.CURSOR_LEN => .{ .ns = std.mem.readInt(u64, cursor[0..8], .big), .ual = std.mem.readInt(u64, cursor[8..16], .big) },
            else => return badRead(std.fmt.comptimePrint("bad request: a cursor is the {d} bytes a page answered", .{ts_ops.CURSOR_LEN})),
        };
        const min: u64 = if (from_ns) |t| @intCast(t) else 0;
        // Apply refuses a point past maxInt(i64) ns: the answer's time is i64.
        const max: u64 = if (to_ns) |t| @intCast(t) else std.math.maxInt(i64);
        // One past the page, to know whether more follow.
        const buf = self.allocator.alloc(StoredPoint, limit + 1) catch
            return .{ .refused = .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "ts read: out of memory" } } };
        defer self.allocator.free(buf);
        const n = if (min > max) 0 else self.ts.readPage(ns, measurement, field, filter.preds, min, max, @fromBackingInt(@intCast(@backingInt(order))), after, buf) catch
            return .{ .refused = .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "ts read failed" } } };
        const page = buf[0..@min(n, limit)];
        var w: schema.Writer = .{ .buf = &self.read_buf };
        for (page) |p| ts_ops.Point.encode(&w, .{ .time_ns = @intCast(p.timestamp_ns), .value = p.field_value }) catch unreachable; // sized for MAX_READ
        var next: ?[]const u8 = null;
        if (n > limit) {
            const last = page[page.len - 1];
            std.mem.writeInt(u64, self.cursor_buf[0..8], last.timestamp_ns, .big);
            std.mem.writeInt(u64, self.cursor_buf[8..16], last.ual_index, .big);
            next = &self.cursor_buf;
        }
        return .{ .answer = .{ .points = .{ .bytes = self.read_buf[0..w.pos], .count = @intCast(page.len) }, .cursor = next } };
    }

    // ── Pre-Route ───────────────────────────────────────────────────────

    fn preRouteByMeasurement(req: Request) ?u64 {
        if (req.key.len == 0) return null;
        return router.routeNsKey(req.ns_id, req.key);
    }

    /// A FloQL query carries its measurement in the query, not the key: it
    /// runs on that measurement's shard. A query with none runs on group 0,
    /// where it is refused.
    fn preRouteByQueryMeasurement(req: Request) ?u64 {
        const measurement = floql_parser.Parser.sourceMeasurement(req.value) orelse return null;
        return router.routeNsKey(req.ns_id, measurement);
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
        ns: dispatcher_mod.WalkNamespace,
        _: []const u8,
        _: ?[]const u8,
        _: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const ts: *TSProjection = @ptrCast(@alignCast(ctx));
        const name_buf = dispatcher_mod.scanScratch(ts.seriesCount());
        const count = ts.scanMeasurementNames(ns.id, name_buf);
        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    /// A page of measurements as the list answers them, each
    /// `[name_len:u16][name][series:u32][fields:u32][fields_capped:u8][points:u64]`.
    /// The counts are exact, so fields_capped is 0; ts.info's isn't.
    fn writeListEntries(ctx: *anyopaque, ns: dispatcher_mod.WalkNamespace, names: []const []const u8, allocator: Allocator, out: *std.ArrayList(u8), ends: *std.ArrayList(u32)) error{OutOfMemory}!void {
        const ts: *const TSProjection = @ptrCast(@alignCast(ctx));
        const counts = try allocator.alloc(TSProjection.ListCounts, names.len);
        defer allocator.free(counts);
        try ts.listCounts(ns.id, names, allocator, counts);
        try ends.ensureTotalCapacity(allocator, names.len);
        for (names, counts) |name, c| {
            var head: [2]u8 = undefined;
            std.mem.writeInt(u16, &head, @intCast(name.len), .little);
            try out.appendSlice(allocator, &head);
            try out.appendSlice(allocator, name);
            var tail: [4 + 4 + 1 + 8]u8 = undefined;
            std.mem.writeInt(u32, tail[0..4], c.series, .little);
            std.mem.writeInt(u32, tail[4..8], c.fields, .little);
            tail[8] = 0;
            std.mem.writeInt(u64, tail[9..17], c.points, .little);
            try out.appendSlice(allocator, &tail);
            ends.appendAssumeCapacity(@intCast(out.items.len));
        }
    }

    fn dispatchTS(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.ts_handler.handleCommand(req);
        defer shard.ts_handler.freeResult(result);
        if (result == .parked) {
            const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
            // A write counts against its namespace here, where it is accepted.
            if (op == .ts_write) shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
            return shard.park(conn, req, result.parked, if (op == .ts_write) answerWrite else answerOk);
        }
        switch (result) {
            .ts_write_ok => shard.namespace_handler.markNamespaceHasData(req.namespace, shard),
            else => {},
        }
        sendTSResponse(shard, conn, req.header.request_id, result);
    }

    /// A parked delete or retention applied: nothing to report but that.
    fn answerOk(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        sendTSResponse(shard, conn, request_id, .ok);
    }

    /// A parked write applied: the point's timestamp, as its applier
    /// recorded it; its sequence the entry's index.
    fn answerWrite(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        const key = r.slice() orelse "";
        sendTSResponse(shard, conn, request_id, writeOk(key, r.int(u64) orelse result.timestamp_ns, result.index));
    }

    /// The time the client gave its point, in ns; null when it left the
    /// choice to the server (no option, or 0). An i64, so never past what a
    /// read answers; one before 1970, or not 8 bytes, is refused.
    fn clientTimestampNs(req: Request) error{ BeforeEpoch, NotI64 }!?u64 {
        const opt = req.findOption(.ts_timestamp_ns) orelse return null;
        const ns = opt.asI64() orelse return error.NotI64;
        if (ns < 0) return error.BeforeEpoch;
        return if (ns == 0) null else @intCast(ns);
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    fn handleCommand(self: *TSHandler, req: Request) CommandResult {
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        return switch (op) {
            .ts_write => self.handleWrite(req),
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

        if (req.value.len != 8) {
            return .{ .err = .{ .code = .invalid_request, .message = "ts write: value must be 8 bytes (f64, little-endian)" } };
        }
        const value: f64 = @bitCast(std.mem.readInt(u64, req.value[0..8], .little));
        if (!std.math.isFinite(value)) {
            return .{ .err = .{ .code = .invalid_request, .message = "ts write: value must be a finite number" } };
        }

        // Timestamp: from options, or server-generated
        const client_ts = clientTimestampNs(req) catch |err| return .{ .err = .{ .code = .invalid_request, .message = switch (err) {
            error.BeforeEpoch => "ts write: a point's time must not be before 1970",
            error.NotI64 => "ts write: the timestamp option must be an 8-byte i64 of nanoseconds",
        } } };

        // Canonical tag hash — sorted pairs, so tag ordering is not part of
        // the series identity (see ts_mod.canonicalTagHash).
        const tags_str: []const u8 = if (req.findOption(.ts_tags)) |opt| opt.asString() else "";

        if (self.shard_ptr) |sptr| return proposeWrite(shardFromPtr(sptr), req.namespace, measurement, field_name, value, client_ts, tags_str, entry_mod.Flags.NONE);
        // No shard (unit tests): insert directly (the projection
        // canonicalizes the tag set and owns hashing + dictionary
        // registration).
        const timestamp_ns = client_ts orelse serverTimestampNs();
        const ual_index = self.nextUalIndex();
        self.ts.insert(req.ns_id, measurement, field_name, value, timestamp_ns, ual_index, tags_str) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts write failed" } };
        };
        return writeOk(measurement, timestamp_ns, ual_index);
    }

    /// A point, proposed as a client's write is: a sink's write on the
    /// shard that owns its measurement comes here too, with `Flags.SINK`.
    /// A null `timestamp_ns` takes the entry's stamp.
    pub fn proposeWrite(shard: *Shard, namespace: []const u8, measurement: []const u8, field_name: []const u8, value: f64, timestamp_ns: ?u64, tags: []const u8, flags: u16) CommandResult {
        // The value carries the reading AND the field name, client timestamp and
        // tag set (ts_mod.TsWriteValue). Persisting only the 8 value bytes — as
        // this did — meant replay reconstructed every point as `<measurement>/
        // value` at the entry's own timestamp with no tags, silently losing
        // three of the four dimensions on restart.
        var val_buf: [ts_mod.MAX_TAG_STRING + 512]u8 = undefined;
        const encoded = ts_mod.TsWriteValue.encode(&val_buf, .{
            .value = value,
            // 0: the point takes the entry's stamp.
            .timestamp_ns = timestamp_ns orelse 0,
            .field_name = field_name,
            .tags = tags,
        }) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "ts write: field/tags too large" } };
        };
        // A point without its own timestamp takes the entry's stamp,
        // which the responder answers with.
        const proposed = persistence_mod.proposeEntry(shard, .ts_write, flags, namespace, measurement, encoded) catch |err| {
            return .{ .err = persistence_mod.failureError(err, "ts write not persisted") };
        };
        return .{ .parked = proposed };
    }

    fn writeOk(measurement: []const u8, timestamp_ns: u64, sequence: u64) CommandResult {
        return .{ .ts_write_ok = .{
            .series_hash = std.hash.Wyhash.hash(0, measurement),
            .timestamp_ns = @intCast(timestamp_ns),
            .sequence = sequence,
        } };
    }

    // ── READ ────────────────────────────────────────────────────────────

    /// Parse a `k=v,k=v` filter string into equality predicates.
    ///
    /// Predicates constrain only the tags they name, so `--tags host=web-01`
    /// now matches a point tagged `host=web-01,env=prod`. Under the previous
    /// exact tag-hash lookup a partial filter matched nothing at all.
    fn parseTagPredicates(tags: []const u8, out: []ts_mod.TagPredicate) []const ts_mod.TagPredicate {
        var n: usize = 0;
        var it = std.mem.splitScalar(u8, tags, ',');
        while (it.next()) |raw| {
            if (n >= out.len) break;
            const part = std.mem.trim(u8, raw, " \t");
            if (part.len == 0) continue;
            const eq = std.mem.indexOfScalar(u8, part, '=') orelse continue;
            out[n] = .{ .key = part[0..eq], .op = .eq, .value = part[eq + 1 ..] };
            n += 1;
        }
        return out[0..n];
    }

    /// Tag predicates for a read request (empty = every tag-series).
    fn requestTagPredicates(req: Request, out: []ts_mod.TagPredicate) []const ts_mod.TagPredicate {
        const opt = req.findOption(.ts_tags) orelse return &.{};
        const tags_str = opt.asString();
        if (tags_str.len == 0) return &.{};
        return parseTagPredicates(tags_str, out);
    }

    /// Translate FloQL source tag filters into predicates. Every operator maps
    /// directly now — `=~`/`!~` are glob, matching the `{host=~web-*}` form the
    /// e2e suite uses — so filters are no longer silently dropped for anything
    /// other than a complete `=` set.
    fn floqlTagPredicates(filters: []const floql_ast.TagFilter, out: []ts_mod.TagPredicate) []const ts_mod.TagPredicate {
        var n: usize = 0;
        for (filters) |f| {
            if (n >= out.len) break;
            out[n] = .{
                .key = f.key,
                .op = switch (f.op) {
                    .eq => .eq,
                    .neq => .neq,
                    .regex => .glob,
                    .nregex => .nglob,
                },
                .value = f.value,
            };
            n += 1;
        }
        return out[0..n];
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

        // Times and the window in ns; 0 (or none) leaves the bound open.
        const from_ns: u64 = switch (nsOption(req, .ts_from_ns)) {
            .bad => |why| return .{ .err = .{ .code = .invalid_request, .message = why } },
            .none => 0,
            .ns => |t| t,
        };
        const to_ns: u64 = switch (nsOption(req, .ts_to_ns)) {
            .bad => |why| return .{ .err = .{ .code = .invalid_request, .message = why } },
            .none => std.math.maxInt(u64),
            .ns => |t| t,
        };

        const agg_name = if (req.findOption(.ts_aggregation)) |opt|
            opt.asString()
        else
            "avg";

        // Buckets are aligned to the epoch (ts / window * window) so a bucket's
        // identity does not depend on the query's `from`.
        // A window given as 0 is refused, not taken as the default: a client
        // still sending ms would otherwise get windows a million times too
        // narrow, or none.
        const window_ns: u64 = if (req.findOption(.ts_window_ns) == null) DEFAULT_WINDOW_NS else switch (nsOption(req, .ts_window_ns)) {
            .bad => |why| return .{ .err = .{ .code = .invalid_request, .message = why } },
            .none => return .{ .err = .{ .code = .invalid_request, .message = "ts query: a window must be more than 0 ns" } },
            .ns => |w| w,
        };

        const ns_id = req.ns_id;
        var pred_buf: [32]ts_mod.TagPredicate = undefined;
        const preds = requestTagPredicates(req, &pred_buf);

        // Every point in the range, up to `MAX_QUERY_SCANNED`, past which
        // the query is refused: points arrive time-ordered, so buckets are
        // produced in order and a point can only ever extend the most
        // recent bucket.
        const Buckets = struct {
            window_ns: u64,
            starts: [MAX_QUERY_BUCKETS]u64 = undefined,
            accs: [MAX_QUERY_BUCKETS]BucketAcc = undefined,
            nb: usize = 0,
            scanned: usize = 0,
            max: usize,
            too_many_windows: bool = false,

            fn add(acc: *@This(), pt: StoredPoint) bool {
                acc.scanned += 1;
                if (acc.scanned > acc.max) return false;
                const start = pt.timestamp_ns / acc.window_ns * acc.window_ns;
                if (acc.nb > 0 and acc.starts[acc.nb - 1] == start) {
                    acc.accs[acc.nb - 1].add(pt.field_value);
                    return true;
                }
                if (acc.nb >= MAX_QUERY_BUCKETS) {
                    acc.too_many_windows = true;
                    return false;
                }
                acc.starts[acc.nb] = start;
                acc.accs[acc.nb] = .{};
                acc.accs[acc.nb].add(pt.field_value);
                acc.nb += 1;
                return true;
            }
        };
        var b: Buckets = .{ .window_ns = window_ns, .max = self.query_scanned_max };
        self.ts.scanFiltered(ns_id, measurement, field_name, preds, from_ns, to_ns, &b, Buckets.add) catch {
            return .{ .err = .{ .code = .internal_error, .message = "ts query failed" } };
        };
        if (b.scanned > b.max) return .{ .err = .{ .code = .invalid_request, .message = TOO_MANY_QUERY_POINTS } };
        if (b.too_many_windows) return .{ .err = .{ .code = .invalid_request, .message = TOO_MANY_QUERY_BUCKETS } };
        const starts = b.starts[0..b.nb];
        const accs = b.accs[0..b.nb];
        const nb = b.nb;

        var buckets: [MAX_QUERY_BUCKETS]QueryBucket = undefined;
        var out_n: usize = 0;
        for (starts[0..nb], accs[0..nb]) |start, acc| {
            if (acc.value(agg_name)) |v| {
                buckets[out_n] = .{ .start_ns = @intCast(start), .value = v };
                out_n += 1;
            }
        }

        const data = serializeQueryResult(self.allocator, measurement, buckets[0..out_n]) catch {
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

        const from_ns: u64, const to_ns: u64 = switch (floqlRange(query.source.range, @import("stdx").time.milliTimestamp(), &self.why_buf)) {
            .bad => |why| return .{ .err = .{ .code = .invalid_request, .message = why } },
            .ok => |r| .{ r.from_ns, r.to_ns },
        };

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

        // 3. Every matching point, so a stage sees them all: up to
        // `MAX_FLOQL_POINTS`, past which the query is refused, not cut short.
        var pred_buf: [32]ts_mod.TagPredicate = undefined;
        const src_preds = floqlTagPredicates(query.source.filters, &pred_buf);
        const Collect = struct {
            list: std.ArrayListUnmanaged(ss_mod.DataPoint) = .empty,
            allocator: Allocator,
            failed: bool = false,
            max: usize,

            fn take(c: *@This(), sp: StoredPoint) bool {
                if (c.list.items.len > c.max) return false;
                c.list.append(c.allocator, .{ .timestamp_ns = @intCast(sp.timestamp_ns), .value = sp.field_value }) catch {
                    c.failed = true;
                    return false;
                };
                return true;
            }
        };
        var collect: Collect = .{ .allocator = self.allocator, .max = self.floql_points_max };
        self.ts.scanFiltered(req.ns_id, measurement, field_name, src_preds, from_ns, to_ns, &collect, Collect.take) catch {
            collect.list.deinit(self.allocator);
            return .{ .err = .{ .code = .internal_error, .message = "floql: ts query failed" } };
        };
        if (collect.failed) {
            collect.list.deinit(self.allocator);
            return .{ .err = .{ .code = .internal_error, .message = "floql: out of memory" } };
        }
        if (collect.list.items.len > collect.max) {
            collect.list.deinit(self.allocator);
            return .{ .err = .{ .code = .invalid_request, .message = TOO_MANY_FLOQL_SCANNED } };
        }

        // 4. Convert the points → SeriesSet (initial pipeline input)
        const dp_slice = collect.list.toOwnedSlice(self.allocator) catch {
            collect.list.deinit(self.allocator);
            return .{ .err = .{ .code = .internal_error, .message = "floql: out of memory" } };
        };

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
        var result_set = floql_executor.execute(query.stages, initial, self.allocator) catch |err| {
            initial.deinit();
            if (err == error.InvalidInput) return .{ .err = .{ .code = .invalid_request, .message = "floql: a stage argument is out of range (percentile 0-100, at most 1000000 buckets per series)" } };
            return .{ .err = .{ .code = .internal_error, .message = "floql: execution failed" } };
        };
        defer result_set.deinit();

        // If pipeline had stages, initial is a separate allocation — free it
        if (query.stages.len > 0) {
            initial.deinit();
        }

        // An answer of more raw points than a read returns is refused, as a
        // read's is: aggregate them, or narrow the query.
        var answer_points: usize = 0;
        for (result_set.series) |sr| answer_points += sr.points.len;
        if (answer_points > MAX_READ_POINTS) return .{ .err = .{ .code = .invalid_request, .message = TOO_MANY_FLOQL_POINTS } };

        // 6. Encode SeriesSet → wire bytes
        const encoded = result_set.encode(self.allocator) catch {
            return .{ .err = .{ .code = .internal_error, .message = "floql: encoding failed" } };
        };

        return .{ .ts_floql_result = .{ .data = encoded } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *TSHandler, req: Request) CommandResult {
        const names = self.ts.listMeasurements(req.ns_id, self.allocator) catch {
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

        // With tags, only the series whose tags include every given pair.
        const raw = if (req.findOption(.ts_tags)) |opt| opt.asString() else "";
        var canon_buf: [ts_mod.MAX_TAG_STRING]u8 = undefined;
        var preds_buf: [ts_mod.MAX_TAGS]ts_mod.TagPredicate = undefined;
        const filter = switch (ts_mod.parseTagFilter(raw, &canon_buf, &preds_buf)) {
            .ok => |f| f,
            .not_pair => |part| return self.refuse(.invalid_request, "tag filter part '{s}' is not key=value", .{part}, "a tag filter part is not key=value"),
            .too_many => return .{ .err = .{ .code = .invalid_request, .message = std.fmt.comptimePrint("a tag filter takes at most {d} tags", .{ts_mod.MAX_TAGS}) } },
            .too_long => return .{ .err = .{ .code = .invalid_request, .message = std.fmt.comptimePrint("a tag filter is at most {d} bytes", .{ts_mod.MAX_TAG_STRING}) } },
        };
        const ns_id = req.ns_id;

        // Through the log, so the delete survives a restart and every
        // replica applies it. Without a shard (unit tests), applied here.
        if (self.shard_ptr) |sptr| {
            const proposed = persistence_mod.proposeEntry(shardFromPtr(sptr), .ts_delete, entry_mod.Flags.NONE, req.namespace, req.key, filter.canonical) catch |err| {
                return .{ .err = persistence_mod.failureError(err, "ts delete not persisted") };
            };
            return .{ .parked = proposed };
        }
        if (self.ts.deleteSeries(ns_id, req.key, filter.preds) == 0 and filter.preds.len > 0) {
            return self.refuse(.not_found, "no series of '{s}' has tags {s}; nothing deleted", .{ req.key, filter.canonical }, "no series has those tags; nothing deleted");
        }
        return .ok;
    }

    // ── RETENTION ───────────────────────────────────────────────────────

    fn handleRetention(self: *TSHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "measurement name is required" } };
        }
        if (req.findOption(.ts_downsample) != null) {
            return .{ .err = .{ .code = .invalid_request, .message = "downsampling isn't supported; retention deletes points older than a duration, once" } };
        }
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

        const age_ms: u64 = @intCast(duration_ms);
        if (age_ms > time_units.MAX_AGE_MS) {
            return .{ .err = .{ .code = .invalid_request, .message = "retention duration is over 100 years" } };
        }

        // The entry carries the duration; each replica cuts at its stamp.
        if (self.shard_ptr) |sptr| {
            var age: [8]u8 = undefined;
            std.mem.writeInt(u64, &age, age_ms, .little);
            const proposed = persistence_mod.proposeEntry(shardFromPtr(sptr), .ts_retention, entry_mod.Flags.NONE, req.namespace, req.key, &age) catch |err| {
                return .{ .err = persistence_mod.failureError(err, "ts retention not persisted") };
            };
            return .{ .parked = proposed };
        }
        _ = self.ts.applyRetention(req.ns_id, req.key, ts_mod.retentionCutoffNs(serverTimestampNs(), age_ms));
        return .ok;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    pub fn nextUalIndex(self: *TSHandler) u64 {
        const idx = self.next_ual_index;
        self.next_ual_index += 1;
        return idx;
    }

    fn serverTimestampNs() u64 {
        return @intCast(@import("stdx").time.nanoTimestamp());
    }

    pub fn freeResult(self: *TSHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .ts_query_result => |r| self.allocator.free(r.data),
            .ts_list_result => |r| self.allocator.free(r.data),
            .ts_floql_result => |r| self.allocator.free(r.data),
            else => {},
        }
    }

    // ── Helpers ─────────────────────────────────────────────

    /// A refusal naming the request's input, or `fallback` if that doesn't fit.
    fn refuse(self: *TSHandler, code: CommandResult.ErrorCode, comptime fmt: []const u8, args: anytype, comptime fallback: []const u8) CommandResult {
        const msg = std.fmt.bufPrint(&self.why_buf, fmt, args) catch fallback;
        return .{ .err = .{ .code = code, .message = msg } };
    }

    /// Cast opaque shard pointer to Shard for proposing entries.
    fn shardFromPtr(ptr: *anyopaque) *Shard {
        return @ptrCast(@alignCast(ptr));
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Dispatch
// ═══════════════════════════════════════════════════════════════════════════════

fn sendTSResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            shard.sendRefusal(conn, request_id, e.code.toStatus(), e.refusal());
        },
        .ts_write_ok => |w| {
            // [series_hash:u64][timestamp_ns:i64][sequence:u64]
            var buf: [24]u8 = undefined;
            std.mem.writeInt(u64, buf[0..8], w.series_hash, .little);
            std.mem.writeInt(i64, buf[8..16], w.timestamp_ns, .little);
            std.mem.writeInt(u64, buf[16..24], w.sequence, .little);
            shard.sendOkResponse(conn, request_id, &buf);
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

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization
// ═══════════════════════════════════════════════════════════════════════════════

/// Past this a bare FloQL time, epoch ms, is likely a ns or us one pasted.
const FLOQL_LATEST_MS: i64 = 32_503_680_000_000; // 3000-01-01

/// A FloQL range in ns: a duration back from now; two durations back from
/// now; or bare numbers, epoch ms. One whose start is after its end is
/// refused, not read as empty.
fn floqlRange(range: floql_ast.TimeRange, now_ms: i64, buf: []u8) union(enum) { ok: struct { from_ns: u64, to_ns: u64 }, bad: []const u8 } {
    var from_ms: i64 = 0;
    var to_ms: ?i64 = null;
    if (range.duration_ms > 0) {
        from_ms = now_ms -| range.duration_ms;
        to_ms = now_ms;
    } else if (range.relative) {
        from_ms = now_ms -| range.from_ms;
        to_ms = now_ms -| range.to_ms;
    } else if (range.from_ms > 0) {
        for ([_]i64{ range.from_ms, range.to_ms }) |t| if (t > FLOQL_LATEST_MS)
            return .{ .bad = std.fmt.bufPrint(buf, "floql: {d} is past the year 3000 as epoch milliseconds; a bare time in a range is ms (did you paste ns or us?)", .{t}) catch "floql: a bare time in a range is epoch ms, and this one is past the year 3000" };
        from_ms = range.from_ms;
        if (range.to_ms > 0) to_ms = range.to_ms;
    }
    if (to_ms) |t| if (from_ms > t) return .{ .bad = "floql: the range's start is after its end" };
    return .{ .ok = .{
        .from_ns = if (from_ms > 0) time_units.msToNsSat(@intCast(from_ms)) else 0,
        .to_ns = if (to_ms) |t| (if (t > 0) time_units.msToNsSat(@intCast(t)) else 0) else std.math.maxInt(u64),
    } };
}

/// Default aggregation window when the request does not specify one.
const DEFAULT_WINDOW_NS: u64 = std.time.ns_per_min;

/// A query's time or window option, in ns: absent or 0 is none. One that
/// isn't an 8-byte i64, or is negative, is refused rather than ignored.
fn nsOption(req: Request, tag: proto.OptionTag) union(enum) { none, ns: u64, bad: []const u8 } {
    const opt = req.findOption(tag) orelse return .none;
    const v = opt.asI64() orelse return .{ .bad = "ts query: a time or window option must be an 8-byte i64 of nanoseconds" };
    if (v < 0) return .{ .bad = "ts query: a time or window must not be negative" };
    return if (v == 0) .none else .{ .ns = @intCast(v) };
}

/// Upper bound on buckets returned by one `ts query`.
const MAX_QUERY_BUCKETS: usize = 1024;

const QueryBucket = struct {
    start_ns: i64,
    value: f64,
};

/// Running aggregate for one window.
const BucketAcc = struct {
    sum: f64 = 0,
    min_v: f64 = std.math.floatMax(f64),
    max_v: f64 = -std.math.floatMax(f64),
    n: usize = 0,

    fn add(self: *BucketAcc, v: f64) void {
        self.sum += v;
        self.n += 1;
        if (v < self.min_v) self.min_v = v;
        if (v > self.max_v) self.max_v = v;
    }

    /// Null for an empty bucket or an unrecognised aggregation name.
    fn value(self: BucketAcc, agg: []const u8) ?f64 {
        if (self.n == 0) return null;
        if (std.mem.eql(u8, agg, "avg")) return self.sum / @as(f64, @floatFromInt(self.n));
        if (std.mem.eql(u8, agg, "sum")) return self.sum;
        if (std.mem.eql(u8, agg, "min")) return self.min_v;
        if (std.mem.eql(u8, agg, "max")) return self.max_v;
        if (std.mem.eql(u8, agg, "count")) return @floatFromInt(self.n);
        return null;
    }
};

/// Serialize an aggregate query result.
///
/// Wire format (the CLI parser in cli/commands/ts.zig must match byte for byte —
/// it previously read a `[hash:u64]` here that no encoder ever wrote, which
/// misaligned the whole stream and rendered an empty table):
///   [series_count:u32][key_len:u32][key][bucket_count:u32]([start_ns:i64][value:f64])*
fn serializeQueryResult(allocator: Allocator, measurement: []const u8, buckets: []const QueryBucket) ![]u8 {
    if (buckets.len == 0) {
        const buf = try allocator.alloc(u8, 4);
        std.mem.writeInt(u32, buf[0..4], 0, .little); // series_count = 0
        return buf;
    }

    const total = 4 + 4 + measurement.len + 4 + buckets.len * 16;
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);
    var offset: usize = 0;

    std.mem.writeInt(u32, buf[offset..][0..4], 1, .little); // series_count
    offset += 4;
    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(measurement.len), .little);
    offset += 4;
    @memcpy(buf[offset .. offset + measurement.len], measurement);
    offset += measurement.len;
    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(buckets.len), .little);
    offset += 4;
    for (buckets) |b| {
        std.mem.writeInt(i64, buf[offset..][0..8], b.start_ns, .little);
        offset += 8;
        std.mem.writeInt(u64, buf[offset..][0..8], @bitCast(b.value), .little);
        offset += 8;
    }

    return buf;
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

fn f64Bytes(v: f64) [8]u8 {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, @bitCast(v), .little);
    return b;
}

fn makeRequest(op: OpCode, key: []const u8, value: []const u8, options: []const u8) Request {
    return .{
        .header = proto.RequestHeader.init(op, 1),
        .namespace = "default",
        .ns_id = .default,
        .key = key,
        .value = value,
        .options = options,
    };
}

test "ts handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    TSHandler.register(&dispatcher);

    try testing.expect(dispatcher.hasHandler(OpCode.ts_write));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_read));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_query));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_floql));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_list));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_delete));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_retention));
    try testing.expect(dispatcher.hasHandler(OpCode.ts_info));

    try testing.expectEqual(@as(u16, 8), dispatcher.handler_count);
}

test "ts handler: write with string value" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(42.5), ""));
    switch (result) {
        .ts_write_ok => |w| {
            try testing.expect(w.series_hash != 0);
            try testing.expect(w.timestamp_ns > 0);
            try testing.expectEqual(@as(u64, 1), w.sequence);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u64, 1), ts.stats.points_inserted);
}

test "ts handler: write with binary f64 value" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
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
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    var opts_buf: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addString(.ts_field, "usage_percent");
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(82.5), opts));
    switch (result) {
        .ts_write_ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: a write's time is nanoseconds, read back exactly; 0 is the server's, and one before 1970 is refused" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);

    const exact: i64 = 1_700_000_000_123_456_789;
    var opts_buf: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addI64(.ts_timestamp_ns, exact);
    switch (handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(82.5), builder.getOptions()))) {
        .ts_write_ok => |w| try testing.expectEqual(exact, w.timestamp_ns),
        else => return error.TestUnexpectedResult,
    }
    const page = try testRead(&handler, "cpu", .{});
    try testing.expectEqual(@as(u32, 1), page.n);
    try testing.expectEqual(exact, page.times[0]);

    // 0 is the server's time.
    const before: i64 = @intCast(TSHandler.serverTimestampNs());
    var zero = OptionsBuilder.init(&opts_buf);
    try zero.addI64(.ts_timestamp_ns, 0);
    switch (handler.handleCommand(makeRequest(.ts_write, "mem", &f64Bytes(1), zero.getOptions()))) {
        .ts_write_ok => |w| try testing.expect(w.timestamp_ns >= before),
        else => return error.TestUnexpectedResult,
    }
    // Before 1970, or not an i64: refused, not taken as the server's time.
    var neg_buf: [64]u8 = undefined;
    var neg = OptionsBuilder.init(&neg_buf);
    try neg.addI64(.ts_timestamp_ns, -1);
    var short_buf: [64]u8 = undefined;
    var short = OptionsBuilder.init(&short_buf);
    try short.addString(.ts_timestamp_ns, "1234");
    const cases = [_]struct { opts: []const u8, why: []const u8 }{
        .{ .opts = neg.getOptions(), .why = "ts write: a point's time must not be before 1970" },
        .{ .opts = short.getOptions(), .why = "ts write: the timestamp option must be an 8-byte i64 of nanoseconds" },
    };
    for (cases) |c| switch (handler.handleCommand(makeRequest(.ts_write, "mem", &f64Bytes(1), c.opts))) {
        .err => |e| {
            try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code);
            try testing.expectEqualStrings(c.why, e.message);
        },
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(@as(u64, 2), ts.stats.points_inserted);
}

test "ts handler: write empty measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_write, "", &f64Bytes(42.0), ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

/// A read's page as the handler answers it: its points' times and values,
/// and its cursor.
const TestPage = struct {
    times: [ts_ops.MAX_READ]i64 = undefined,
    values: [ts_ops.MAX_READ]f64 = undefined,
    n: u32 = 0,
    cursor: ?[ts_ops.CURSOR_LEN]u8 = null,
};

const ReadArgs = struct {
    field: []const u8 = "value",
    tags: []const u8 = "",
    from_ns: ?i64 = null,
    to_ns: ?i64 = null,
    limit: u32 = ts_ops.MAX_READ,
    order: ts_ops.Order = .oldest_first,
    cursor: []const u8 = "",
};

fn testRead(h: *TSHandler, measurement: []const u8, args: ReadArgs) !TestPage {
    switch (h.readPage(.default, measurement, args.field, args.tags, args.from_ns, args.to_ns, args.limit, args.order, args.cursor)) {
        .answer => |v| {
            var page: TestPage = .{ .n = v.points.count };
            for (0..v.points.count) |i| {
                page.times[i] = std.mem.readInt(i64, v.points.bytes[16 * i ..][0..8], .little);
                page.values[i] = @bitCast(std.mem.readInt(u64, v.points.bytes[16 * i + 8 ..][0..8], .little));
            }
            if (v.cursor) |c| page.cursor = c[0..ts_ops.CURSOR_LEN].*;
            return page;
        },
        .refused => return error.Refused,
        else => return error.TestUnexpectedResult,
    }
}

fn refusalOf(h: *TSHandler, measurement: []const u8, args: ReadArgs) ![]const u8 {
    switch (h.readPage(.default, measurement, args.field, args.tags, args.from_ns, args.to_ns, args.limit, args.order, args.cursor)) {
        .refused => |r| {
            try testing.expectEqual(proto.StatusCode.bad_request, r.status);
            return r.refusal.message;
        },
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: read" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    for ([_]f64{ 10.0, 20.0, 30.0 }) |v| _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(v), ""));
    const all = try testRead(&handler, "cpu", .{});
    try testing.expectEqual(@as(u32, 3), all.n);
    try testing.expect(all.cursor == null);
    // A limit short of them: a page, and a cursor to go on from.
    const two = try testRead(&handler, "cpu", .{ .limit = 2 });
    try testing.expectEqual(@as(u32, 2), two.n);
    const rest = try testRead(&handler, "cpu", .{ .limit = 2, .cursor = &two.cursor.? });
    try testing.expectEqual(@as(u32, 1), rest.n);
    try testing.expectEqual(@as(f64, 30.0), rest.values[0]);
    try testing.expect(rest.cursor == null);
    // A measurement with none: an empty page.
    try testing.expectEqual(@as(u32, 0), (try testRead(&handler, "nonexistent", .{})).n);
}

test "ts handler: a read takes one field, the series a tag filter names, and its time range, ends included" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    try ts.insert(.default, "cpu", "user", 1, 100, 0, "host=a,dc=x");
    try ts.insert(.default, "cpu", "user", 2, 200, 0, "host=b,dc=x");
    try ts.insert(.default, "cpu", "user", 3, 300, 0, "host=a,dc=y");
    try ts.insert(.default, "cpu", "idle", 9, 200, 0, "host=a,dc=x");
    try testing.expectEqual(@as(u32, 3), (try testRead(&handler, "cpu", .{ .field = "user" })).n);
    try testing.expectEqual(@as(u32, 1), (try testRead(&handler, "cpu", .{ .field = "idle" })).n);
    try testing.expectEqual(@as(u32, 0), (try testRead(&handler, "cpu", .{})).n);
    const a = try testRead(&handler, "cpu", .{ .field = "user", .tags = "host=a" });
    try testing.expectEqual(@as(u32, 2), a.n);
    try testing.expectEqual(@as(u32, 1), (try testRead(&handler, "cpu", .{ .field = "user", .tags = "dc=x,host=b" })).n);
    const mid = try testRead(&handler, "cpu", .{ .field = "user", .from_ns = 200, .to_ns = 300 });
    try testing.expectEqual(@as(u32, 2), mid.n);
    try testing.expectEqual(@as(i64, 200), mid.times[0]);
    try testing.expectEqual(@as(u32, 0), (try testRead(&handler, "cpu", .{ .field = "user", .from_ns = 301 })).n);
    try testing.expectEqual(@as(u32, 0), (try testRead(&handler, "cpu", .{ .field = "user", .from_ns = 300, .to_ns = 200 })).n);
}

test "ts handler: a read refuses an empty measurement, a bad tag filter and a cursor it didn't answer" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    try testing.expectEqualStrings("bad request: measurement name is required", try refusalOf(&handler, "", .{}));
    try testing.expectEqualStrings("bad request: tag filter part 'host' is not key=value", try refusalOf(&handler, "cpu", .{ .tags = "host" }));
    try testing.expectEqualStrings("bad request: a cursor is the 16 bytes a page answered", try refusalOf(&handler, "cpu", .{ .cursor = "short" }));
}

test "ts handler: a read reaches the latest time a point can have, and a cursor past it reads no further" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    const latest: u64 = std.math.maxInt(i64);
    try ts.insert(.default, "cpu", "value", 1, 100, 0, "");
    try ts.insert(.default, "cpu", "value", 2, latest, 0, "");
    try testing.expectError(error.Refused, ts.insert(.default, "cpu", "value", 3, latest + 1, 0, ""));
    for ([_]ts_ops.Order{ .oldest_first, .newest_first }) |order| {
        const page = try testRead(&handler, "cpu", .{ .order = order });
        try testing.expectEqual(@as(u32, 2), page.n);
        try testing.expectEqual(@as(i64, std.math.maxInt(i64)), page.times[if (order == .oldest_first) 1 else 0]);
    }
    // A cursor no page answered, past every time: nothing after it, and
    // everything before it.
    var past: [ts_ops.CURSOR_LEN]u8 = undefined;
    std.mem.writeInt(u64, past[0..8], std.math.maxInt(u64), .big);
    std.mem.writeInt(u64, past[8..16], 0, .big);
    try testing.expectEqual(@as(u32, 0), (try testRead(&handler, "cpu", .{ .cursor = &past })).n);
    try testing.expectEqual(@as(u32, 2), (try testRead(&handler, "cpu", .{ .order = .newest_first, .cursor = &past })).n);
}

test "ts handler: query avg" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "disk", &f64Bytes(10.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "disk", &f64Bytes(20.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "disk", &f64Bytes(30.0), ""));

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

/// A command's refusal message, invalid_request; "" when it answers.
fn refusedWith(h: *TSHandler, req: Request) ![]const u8 {
    const result = h.handleCommand(req);
    switch (result) {
        .err => |e| {
            try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code);
            return e.message;
        },
        else => {
            h.freeResult(result);
            return "";
        },
    }
}

/// A query's buckets as `(start_ns, value)`.
fn queryBuckets(h: *TSHandler, measurement: []const u8, opts: []const u8, out: [][2]f64) ![]const [2]f64 {
    const result = h.handleCommand(makeRequest(.ts_query, measurement, "", opts));
    defer h.freeResult(result);
    const data = switch (result) {
        .ts_query_result => |r| r.data,
        else => return error.TestUnexpectedResult,
    };
    if (std.mem.readInt(u32, data[0..4], .little) == 0) return out[0..0];
    const key_len = std.mem.readInt(u32, data[4..8], .little);
    var at: usize = 8 + key_len;
    const n = std.mem.readInt(u32, data[at..][0..4], .little);
    at += 4;
    for (out[0..n]) |*b| {
        b.* = .{ @floatFromInt(std.mem.readInt(i64, data[at..][0..8], .little)), @bitCast(std.mem.readInt(u64, data[at + 8 ..][0..8], .little)) };
        at += 16;
    }
    return out[0..n];
}

test "ts handler: a query takes its range and window in ns, and answers bucket starts in ns" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    const base: u64 = 1_700_000_000_000_000_000;
    // Three points within one millisecond, 400 ns apart.
    for ([_]u64{ 100, 500, 900 }, [_]f64{ 1, 2, 3 }) |off, v| try ts.insert(.default, "disk", "value", v, base + off, 0, "");

    var ob: [96]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addString(.ts_aggregation, "sum");
    try b.addI64(.ts_window_ns, 400);
    var out: [8][2]f64 = undefined;
    // Windows of 400 ns from the epoch: [0,400) [400,800) [800,1200) past base.
    const got = try queryBuckets(&handler, "disk", b.getOptions(), &out);
    try testing.expectEqual(@as(usize, 3), got.len);
    try testing.expectEqual(@as(f64, @floatFromInt(base)), got[0][0]);
    try testing.expectEqual(@as(f64, @floatFromInt(base + 800)), got[2][0]);
    try testing.expectEqual(@as(f64, 3), got[2][1]);

    // A range in ns: from the second point on.
    var rb_buf: [96]u8 = undefined;
    var rb = OptionsBuilder.init(&rb_buf);
    try rb.addString(.ts_aggregation, "count");
    try rb.addI64(.ts_from_ns, @intCast(base + 500));
    const ranged = try queryBuckets(&handler, "disk", rb.getOptions(), &out);
    try testing.expectEqual(@as(usize, 1), ranged.len);
    try testing.expectEqual(@as(f64, 2), ranged[0][1]);

    // A window of 0, or one so narrow the points span more windows than an
    // answer holds (a client still sending ms), is refused.
    var zb_buf: [64]u8 = undefined;
    var zb = OptionsBuilder.init(&zb_buf);
    try zb.addI64(.ts_window_ns, 0);
    try testing.expectEqualStrings("ts query: a window must be more than 0 ns", try refusedWith(&handler, makeRequest(.ts_query, "disk", "", zb.getOptions())));
    for (0..MAX_QUERY_BUCKETS + 1) |i| try ts.insert(.default, "many", "value", 1, base + i * 1000, 0, "");
    var nb_buf: [64]u8 = undefined;
    var nb = OptionsBuilder.init(&nb_buf);
    try nb.addI64(.ts_window_ns, 1000);
    try testing.expectEqualStrings(TOO_MANY_QUERY_BUCKETS, try refusedWith(&handler, makeRequest(.ts_query, "many", "", nb.getOptions())));

    // A negative time, or one that isn't 8 bytes, is refused.
    for ([_]struct { tag: proto.OptionTag, neg: bool }{ .{ .tag = .ts_from_ns, .neg = true }, .{ .tag = .ts_window_ns, .neg = true }, .{ .tag = .ts_to_ns, .neg = false } }) |c| {
        var bad_buf: [64]u8 = undefined;
        var bad = OptionsBuilder.init(&bad_buf);
        if (c.neg) try bad.addI64(c.tag, -1) else try bad.addString(c.tag, "1234");
        switch (handler.handleCommand(makeRequest(.ts_query, "disk", "", bad.getOptions()))) {
            .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
            else => return error.TestUnexpectedResult,
        }
    }
}

test "ts handler: a floql range counts back from now, takes bare numbers as epoch ms, and refuses one backwards or past the year 3000" {
    var buf: [256]u8 = undefined;
    const now: i64 = 1_700_000_000_000;
    const ms = std.time.ns_per_ms;
    const R = floql_ast.TimeRange;
    const Case = struct { range: R, from: u64, to: u64 };
    const cases = [_]Case{
        .{ .range = .{ .duration_ms = 3_600_000 }, .from = (now - 3_600_000) * ms, .to = now * ms },
        // [2h..1h]: two hours ago to one hour ago.
        .{ .range = .{ .from_ms = 7_200_000, .to_ms = 3_600_000, .relative = true }, .from = (now - 7_200_000) * ms, .to = (now - 3_600_000) * ms },
        .{ .range = .{ .from_ms = 1_000, .to_ms = 2_000 }, .from = 1_000 * ms, .to = 2_000 * ms },
        // One bare number: a lower bound.
        .{ .range = .{ .from_ms = 1_000 }, .from = 1_000 * ms, .to = std.math.maxInt(u64) },
    };
    for (cases) |c| {
        const r = floqlRange(c.range, now, &buf).ok;
        try testing.expectEqual(c.from, r.from_ns);
        try testing.expectEqual(c.to, r.to_ns);
    }
    try testing.expectEqualStrings("floql: the range's start is after its end", floqlRange(.{ .from_ms = 3_600_000, .to_ms = 7_200_000, .relative = true }, now, &buf).bad);
    try testing.expectEqualStrings("floql: the range's start is after its end", floqlRange(.{ .from_ms = 2_000, .to_ms = 1_000 }, now, &buf).bad);
    try testing.expect(std.mem.startsWith(u8, floqlRange(.{ .from_ms = 1_708_700_400_000_000_000 }, now, &buf).bad, "floql: 1708700400000000000 is past the year 3000 as epoch milliseconds"));
}

test "ts handler: a floql answer's points carry their time in ns" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    const t: u64 = 1_700_000_000_123_456_789;
    try ts.insert(.default, "cpu", "value", 7, t, 0, "");
    const result = handler.handleCommand(makeRequest(.ts_floql, "", "cpu[1700000000000]", ""));
    defer handler.freeResult(result);
    const data = switch (result) {
        .ts_floql_result => |r| r.data,
        else => return error.TestUnexpectedResult,
    };
    // [series:u32][key_len:u32][key][field_len:u32][field][points:u32][ns:i64][value:f64]
    var at: usize = 4;
    at += 4 + std.mem.readInt(u32, data[at..][0..4], .little);
    at += 4 + std.mem.readInt(u32, data[at..][0..4], .little);
    try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, data[at..][0..4], .little));
    try testing.expectEqual(@as(i64, @intCast(t)), std.mem.readInt(i64, data[at + 4 ..][0..8], .little));
}

test "ts handler: query count" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "net", &f64Bytes(1.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "net", &f64Bytes(2.0), ""));

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
    var ts = TSProjection.init(allocator);
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
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(1.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "memory", &f64Bytes(2.0), ""));

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
    try testing.expectEqual(@as(?u64, null), TSHandler.preRouteByMeasurement(req_empty));

    // Same measurement, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.ts_write, "cpu", "", "");
    req_ns.namespace = "other";
    req_ns.ns_id = .of(16);
    try testing.expect(TSHandler.preRouteByMeasurement(req1) != TSHandler.preRouteByMeasurement(req_ns));
}

test "ts handler: write refuses a value that is not one finite f64" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const bad = [_][]const u8{ "", "1234567", "123456789", "1234.5678", &f64Bytes(std.math.nan(f64)), &f64Bytes(std.math.inf(f64)), &f64Bytes(-std.math.inf(f64)) };
    for (bad) |v| {
        switch (handler.handleCommand(makeRequest(.ts_write, "cpu", v, ""))) {
            .err => |e| try testing.expectEqual(@as(@TypeOf(e.code), .invalid_request), e.code),
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(u64, 0), ts.stats.points_inserted);
}

test "ts handler: write accepts negative zero and subnormals" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);
    for ([_]f64{ -0.0, std.math.floatTrueMin(f64), -std.math.floatTrueMin(f64) }) |v| {
        switch (handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(v), ""))) {
            .ts_write_ok => {},
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(u64, 3), ts.stats.points_inserted);
}

test "ts handler: multiple writes same measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    for (0..10) |i| {
        const result = handler.handleCommand(makeRequest(.ts_write, "temp", &f64Bytes(@floatFromInt(i)), ""));
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
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // ok result should be safe to free (no-op)
    handler.freeResult(.ok);
    handler.freeResult(.{ .err = .{ .code = .invalid_request, .message = "test" } });
}

test "ts handler: floql basic pipeline" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // Write data points
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(10.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(20.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(30.0), ""));

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
    var ts = TSProjection.init(allocator);
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
    var ts = TSProjection.init(allocator);
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
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    // Write some data
    for (0..6) |i| {
        _ = handler.handleCommand(makeRequest(.ts_write, "mem", &f64Bytes(@floatFromInt((i + 1) * 10)), ""));
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
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(10.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "cpu", &f64Bytes(20.0), ""));
    _ = handler.handleCommand(makeRequest(.ts_write, "memory", &f64Bytes(30.0), ""));

    try testing.expectEqual(@as(u64, 3), ts.stats.points_inserted);

    // Delete cpu measurement
    const del_result = handler.handleCommand(makeRequest(.ts_delete, "cpu", "", ""));
    switch (del_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u32, 0), (try testRead(&handler, "cpu", .{})).n);
    try testing.expectEqual(@as(u32, 1), (try testRead(&handler, "memory", .{})).n);
}

test "ts handler: delete empty measurement" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
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
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_retention, "", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: a retention duration past 100 years is refused" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);

    // 36,700 days is just past 100 years of 366.
    switch (handler.handleCommand(makeRequest(.ts_retention, "m", "36700d", ""))) {
        .err => |e| try testing.expectEqualStrings("retention duration is over 100 years", e.message),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: retention invalid duration" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();

    var handler = TSHandler.init(allocator, &ts);

    const result = handler.handleCommand(makeRequest(.ts_retention, "", "bad", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "ts handler: a read pages through every point, oldest or newest first, each once, across series and points sharing a time" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    // Three hosts, every point time shared by two of them.
    const n = MAX_READ_POINTS + 500;
    for (0..n) |i| {
        const hosts = [_][]const u8{ "host=a", "host=b", "host=c" };
        const host = hosts[i % 3];
        try ts.insert(.default, "cpu", "value", @floatFromInt(i), 1000 + i / 2, 0, host);
    }
    // A default page is the most a page holds, and says where to go on.
    const first = try testRead(&handler, "cpu", .{});
    try testing.expectEqual(MAX_READ_POINTS, first.n);
    try testing.expect(first.cursor != null);
    inline for (.{ ts_ops.Order.oldest_first, ts_ops.Order.newest_first }) |order| {
        var seen = try allocator.alloc(bool, n);
        defer allocator.free(seen);
        @memset(seen, false);
        var cursor: [ts_ops.CURSOR_LEN]u8 = undefined;
        var cur: []const u8 = "";
        var count: usize = 0;
        var prev: ?i64 = null;
        while (true) {
            const page = try testRead(&handler, "cpu", .{ .limit = 777, .order = order, .cursor = cur });
            for (page.times[0..page.n], page.values[0..page.n]) |t, v| {
                const i: usize = @intFromFloat(v);
                try testing.expect(!seen[i]);
                seen[i] = true;
                count += 1;
                if (prev) |p| try testing.expect(if (order == .oldest_first) t >= p else t <= p);
                prev = t;
            }
            cursor = page.cursor orelse break;
            cur = &cursor;
        }
        try testing.expectEqual(n, count);
    }
    // Newest first starts at the newest point; a range bounds it both ways.
    const newest = try testRead(&handler, "cpu", .{ .limit = 1, .order = .newest_first });
    try testing.expectEqual(@as(i64, 1000 + (n - 1) / 2), newest.times[0]);
    const window = try testRead(&handler, "cpu", .{ .from_ns = 1100, .to_ns = 1101, .order = .newest_first });
    try testing.expectEqual(@as(u32, 4), window.n);
    try testing.expectEqual(@as(i64, 1101), window.times[0]);
    try testing.expectEqual(@as(i64, 1100), window.times[3]);
    // Tags select series: a third of them.
    var tagged: usize = 0;
    var tcur: [ts_ops.CURSOR_LEN]u8 = undefined;
    var tc: []const u8 = "";
    while (true) {
        const page = try testRead(&handler, "cpu", .{ .tags = "host=b", .cursor = tc });
        tagged += page.n;
        tcur = page.cursor orelse break;
        tc = &tcur;
    }
    try testing.expectEqual(@as(usize, n / 3), tagged);
}

test "ts handler: FloQL aggregates every matching point, and refuses an answer of more raw points than a read returns" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    const now_ns: u64 = @intCast(@import("stdx").time.milliTimestamp() * std.time.ns_per_ms);
    const n = MAX_READ_POINTS + 904;
    for (0..n) |i| try ts.insert(.default, "cpu", "value", 1, now_ns - i * std.time.ns_per_ms, 0, "");
    {
        const result = handler.handleCommand(makeRequest(.ts_floql, "", "cpu[24h] | count()", ""));
        switch (result) {
            .ts_floql_result => |r| {
                defer handler.freeResult(result);
                // One series, "cpu"/"value", one point: its count.
                const at = 4 + 4 + 3 + 4 + 5 + 4 + 8;
                try testing.expectEqual(@as(f64, n), @as(f64, @bitCast(std.mem.readInt(u64, r.data[at..][0..8], .little))));
            },
            else => return error.TestUnexpectedResult,
        }
    }
    const raw = handler.handleCommand(makeRequest(.ts_floql, "", "cpu[24h]", ""));
    switch (raw) {
        .err => |e| try testing.expectEqualStrings(TOO_MANY_FLOQL_POINTS, e.message),
        else => {
            handler.freeResult(raw);
            return error.TestUnexpectedResult;
        },
    }
}

test "ts handler: a query or FloQL query reading past its budget is refused, not cut short" {
    const allocator = testing.allocator;
    var ts = TSProjection.init(allocator);
    defer ts.deinit();
    var handler = TSHandler.init(allocator, &ts);
    const now_ns: u64 = @intCast(@import("stdx").time.milliTimestamp() * std.time.ns_per_ms);
    for (0..11) |i| try ts.insert(.default, "cpu", "value", 1, now_ns - i * std.time.ns_per_ms, 0, "");
    handler.query_scanned_max = 11;
    handler.floql_points_max = 11;
    try testing.expectEqualStrings("", try refusedWith(&handler, makeRequest(.ts_query, "cpu", "", "")));
    try testing.expectEqualStrings("", try refusedWith(&handler, makeRequest(.ts_floql, "", "cpu[24h] | count()", "")));
    handler.query_scanned_max = 10;
    handler.floql_points_max = 10;
    try testing.expectEqualStrings(TOO_MANY_QUERY_POINTS, try refusedWith(&handler, makeRequest(.ts_query, "cpu", "", "")));
    try testing.expectEqualStrings(TOO_MANY_FLOQL_SCANNED, try refusedWith(&handler, makeRequest(.ts_floql, "", "cpu[24h] | count()", "")));
}
