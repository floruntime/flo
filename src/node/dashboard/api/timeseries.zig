//! Dashboard API — Time-Series Endpoints
//!
//! - GET  /timeseries                       — All measurements with stats
//! - GET  /timeseries/:measurement          — Measurement detail (fields, series count)
//! - GET  /timeseries/:measurement/data     — Query data points (scatter-gather ts_query)
//! - POST /timeseries/floql                 — Execute FloQL query (scatter-gather ts_floql)
//!
//! Reads TS metadata from KV internal keys (_ts:ml:*, _ts:fields:*,
//! _ts:meta:*) by dispatching internal KV commands (connection_id=0).
//! Data queries dispatch ts_query to all shards and merge binary results.

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const Dispatcher = h.Dispatcher;
const MetricsRegistry = h.MetricsRegistry;
const Core = h.Core;
const Command = h.Command;
const Method = @import("../../../util/http/mod.zig").Method;

// TS metadata decoders
const FieldRegistry = @import("../../../ts/metadata.zig").FieldRegistry;
const SeriesMetadata = @import("../../../ts/metadata.zig").SeriesMetadata;
const ts_keys = @import("../../../ts/keys.zig");

// =============================================================================
// GET /timeseries — List all measurements
// =============================================================================

/// GET /timeseries — List all measurements with field counts and stats.
///
/// Scans `_ts:ml:` marker keys across all shards (via scanLocalKVInternal)
/// and for each measurement reads `_ts:fields:{name}` to get field names.
///
/// Namespace is taken from ?namespace= query param (defaults to "default").
pub fn getMeasurements(
    allocator: Allocator,
    query_string: ?[]const u8,
    dispatchers: []*Dispatcher,
    cores: ?[]*Core,
) ![]const u8 {
    _ = cores;
    if (dispatchers.len == 0) return try allocator.dupe(u8, "[]");

    const namespace = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    // Scan _ts:ml: marker keys across all shards
    const measurement_names = readMeasurementList(allocator, dispatchers, namespace) catch |err| {
        log.warn("TS dashboard: failed to read measurement list: {}", .{err});
        return try allocator.dupe(u8, "[]");
    };
    defer {
        for (measurement_names) |m| allocator.free(m);
        allocator.free(measurement_names);
    }

    // Build JSON array
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    for (measurement_names) |name| {
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", name);
        try obj.stringField("namespace", namespace);

        // Read field registry for this measurement
        const fields = readFieldRegistry(allocator, dispatchers, namespace, name) catch null;
        if (fields) |f| {
            defer {
                for (f) |field| allocator.free(field);
                allocator.free(f);
            }
            try obj.intField("field_count", @as(i64, @intCast(f.len)));

            // Write fields as array
            var fields_arr = try obj.arrayField("fields");
            try fields_arr.begin();
            try fields_arr.writeStringSlice(f);
            try fields_arr.end();
        } else {
            try obj.intField("field_count", 0);
            var fields_arr = try obj.arrayField("fields");
            try fields_arr.begin();
            try fields_arr.end();
        }

        // Count series for this measurement by scanning _ts:meta: keys
        // This is approximate — we scan all shards' local KV for _ts: keys
        const series_count = countSeriesForMeasurement(allocator, dispatchers, namespace, name);
        try obj.intField("series_count", @as(i64, @intCast(series_count)));

        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// GET /timeseries/:measurement — Measurement detail
// =============================================================================

/// GET /timeseries/:measurement — Detailed view of a single measurement.
///
/// Returns fields, series metadata, and retention config.
pub fn getMeasurementDetail(
    allocator: Allocator,
    measurement_name: []const u8,
    query_string: ?[]const u8,
    dispatchers: []*Dispatcher,
    cores: ?[]*Core,
) ![]const u8 {
    _ = cores;
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const namespace = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", measurement_name);
    try obj.stringField("namespace", namespace);

    // Fields
    const fields = readFieldRegistry(allocator, dispatchers, namespace, measurement_name) catch null;
    if (fields) |f| {
        defer {
            for (f) |field| allocator.free(field);
            allocator.free(f);
        }
        try obj.intField("field_count", @as(i64, @intCast(f.len)));
        var fields_arr = try obj.arrayField("fields");
        try fields_arr.begin();
        try fields_arr.writeStringSlice(f);
        try fields_arr.end();
    } else {
        try obj.intField("field_count", 0);
        var fields_arr = try obj.arrayField("fields");
        try fields_arr.begin();
        try fields_arr.end();
    }

    // Series metadata — read individual series entries
    const series_list = readSeriesForMeasurement(allocator, dispatchers, namespace, measurement_name) catch &[_]SeriesInfo{};
    defer {
        for (series_list) |s| {
            allocator.free(s.canonical_key);
            for (s.tags) |tag| {
                allocator.free(tag.key);
                allocator.free(tag.value);
            }
            allocator.free(s.tags);
        }
        allocator.free(series_list);
    }

    try obj.intField("series_count", @as(i64, @intCast(series_list.len)));

    // Series array
    var series_arr = try obj.arrayField("series");
    try series_arr.begin();
    for (series_list) |s| {
        try series_arr.next();
        var series_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try series_obj.begin();
        try series_obj.stringField("canonical_key", s.canonical_key);
        try series_obj.intField("approx_count", @as(i64, @intCast(s.approx_count)));
        try series_obj.intField("last_write_ms", s.last_write_ms);
        try series_obj.intField("created_at_ms", s.created_at_ms);

        // Tags
        var tags_obj = try series_obj.objectField("tags");
        try tags_obj.begin();
        for (s.tags) |tag| {
            try tags_obj.stringField(tag.key, tag.value);
        }
        try tags_obj.end();

        try series_obj.end();
    }
    try series_arr.end();

    // Retention config
    const retention_data = readRetentionConfig(allocator, dispatchers, namespace, measurement_name);
    if (retention_data) |data| {
        defer allocator.free(data);
        try obj.stringField("retention", data);
    } else {
        try obj.nullField("retention");
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// GET /timeseries/:measurement/data — Query data points
// =============================================================================

/// GET /timeseries/:measurement/data — Return queried data points as JSON.
///
/// Dispatches `ts_query` to ALL shards (scatter-gather) and merges the binary
/// results into a JSON response suitable for charting.
///
/// Query params:
///   - namespace    (default: "default")
///   - field        (default: first registered field, or "value")
///   - from         epoch ms  (default: now - 1h)
///   - to           epoch ms  (default: now)
///   - window       window ms (default: 60000 = 1 min)
///   - aggregation  avg|sum|count|min|max (default: "avg")
///
/// Response JSON:
/// ```json
/// {
///   "measurement": "cpu_usage",
///   "field": "user",
///   "aggregation": "avg",
///   "window_ms": 60000,
///   "from_ms": 1700000000000,
///   "to_ms":   1700003600000,
///   "series": [
///     {
///       "key": "cpu_usage,host=web-01",
///       "points": [ { "t": 1700000000000, "v": 72.5 }, ... ]
///     }
///   ]
/// }
/// ```
pub fn getSeriesData(
    allocator: Allocator,
    measurement_name: []const u8,
    query_string: ?[]const u8,
    dispatchers: []*Dispatcher,
    cores: ?[]*Core,
) ![]const u8 {
    _ = cores;
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const namespace = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    // Determine field: explicit param, or read first field from registry
    const explicit_field = h.parseQueryParam([]const u8, query_string, "field");
    var field_owned: ?[]const u8 = null;
    defer if (field_owned) |f| allocator.free(@constCast(f));

    const field: []const u8 = if (explicit_field) |ef|
        ef
    else blk: {
        const fields = readFieldRegistry(allocator, dispatchers, namespace, measurement_name) catch null;
        if (fields) |f| {
            defer {
                for (f[1..]) |ff| allocator.free(ff);
                allocator.free(f);
            }
            if (f.len > 0) {
                field_owned = f[0]; // take ownership of first field string
                break :blk field_owned.?;
            }
        }
        break :blk "value";
    };

    // Time range defaults: last 1 hour
    const now_ms: i64 = @intCast(
        @as(u64, @truncate(@as(u128, @intCast(std.time.milliTimestamp())))),
    );
    const from_ms: i64 = h.parseQueryParam(i64, query_string, "from") orelse (now_ms - 3_600_000);
    const to_ms: i64 = h.parseQueryParam(i64, query_string, "to") orelse now_ms;
    const window_ms: i64 = h.parseQueryParam(i64, query_string, "window") orelse 60_000;
    const aggregation = h.parseQueryParam([]const u8, query_string, "aggregation") orelse "avg";

    // --- Scatter-gather: dispatch ts_query to every shard --------------------
    // ts_query is always scatter-gather because tag resolution is per-shard.
    // We collect all binary results and decode them into a unified JSON response.

    const cmd: Command = .{
        .ts_query = .{
            .namespace = namespace,
            .measurement = measurement_name,
            .tags = &.{}, // empty = match all series
            .field = field,
            .from_ms = from_ms,
            .to_ms = to_ms,
            .window_ms = window_ms,
            .aggregation = aggregation,
        },
    };

    // Collect binary results from all shards
    var shard_results: std.ArrayList([]const u8) = .empty;
    defer {
        for (shard_results.items) |data| allocator.free(data);
        shard_results.deinit(allocator);
    }

    for (dispatchers) |d| {
        const result = d.dispatch(cmd, 0, 0, null) catch continue;
        if (result) |res| {
            switch (res) {
                .ts_query_result => |qr| {
                    try shard_results.append(allocator, qr.data);
                },
                .err => |e| {
                    log.warn("TS data query error from shard: {s}", .{e.message});
                },
                else => {},
            }
        }
    }

    // --- Decode binary results and build JSON --------------------------------
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("measurement", measurement_name);
    try obj.stringField("field", field);
    try obj.stringField("aggregation", aggregation);
    try obj.intField("window_ms", window_ms);
    try obj.intField("from_ms", from_ms);
    try obj.intField("to_ms", to_ms);

    var series_arr = try obj.arrayField("series");
    try series_arr.begin();

    // Decode each shard's binary ts_query_result
    for (shard_results.items) |data| {
        var reader = h.BinaryReader.init(data);
        const series_count = reader.readU32() orelse continue;

        var si: u32 = 0;
        while (si < series_count) : (si += 1) {
            // [key_len:u32][key][bucket_count:u32][bucket...]
            const key_len = reader.readU32() orelse break;
            const key_bytes = reader.readBytes(key_len) orelse break;
            const bucket_count = reader.readU32() orelse break;

            try series_arr.next();
            var series_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try series_obj.begin();
            try series_obj.stringField("key", key_bytes);

            var points_arr = try series_obj.arrayField("points");
            try points_arr.begin();

            var bi: u32 = 0;
            while (bi < bucket_count) : (bi += 1) {
                const window_start = reader.readI64() orelse break;
                const raw_val = reader.readBytes(8) orelse break;
                const val: f64 = @bitCast(std.mem.readInt(u64, raw_val[0..8], .little));

                // Skip NaN buckets (no data for this window)
                if (std.math.isNan(val)) {
                    bi += 1;
                    continue;
                }

                try points_arr.next();
                var pt_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try pt_obj.begin();
                try pt_obj.intField("t", window_start);
                try pt_obj.floatField("v", val);
                try pt_obj.end();
            }

            try points_arr.end();
            try series_obj.end();
        }
    }

    try series_arr.end();
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// POST /timeseries/floql — Execute a FloQL query
// =============================================================================

/// POST /timeseries/floql — Execute a FloQL query string.
///
/// Accepts the query as POST body (plain text) or as ?query= parameter.
/// Dispatches `ts_floql` to every shard (scatter-gather), merges binary
/// SeriesSet results, and returns a JSON response compatible with the
/// chart data format used by the Explorer tab.
///
/// Request:  POST /api/v1/timeseries/floql
///   Body:   cpu{host=web-01}[1h] | window(5m) | avg()
///   -or-    GET /api/v1/timeseries/floql?query=cpu%7B...%7D
///
/// Response: { "query": "...", "series": [{ "key": "...", "field": "...", "points": [{"t": ..., "v": ...}] }] }
pub fn executeFloql(
    allocator: Allocator,
    method: Method,
    query_string: ?[]const u8,
    body: []const u8,
    dispatchers: []*Dispatcher,
) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    // Accept query from POST body or GET ?query= parameter
    const query_text: []const u8 = blk: {
        if (method == .POST and body.len > 0) break :blk body;
        if (h.parseQueryParam([]const u8, query_string, "query")) |q| break :blk q;
        return try h.jsonError(allocator, "Missing query: send as POST body or ?query= parameter");
    };

    const namespace = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    // --- Scatter-gather: dispatch ts_floql to every shard --------------------
    const cmd: Command = .{
        .ts_floql = .{
            .namespace = namespace,
            .query_string = query_text,
        },
    };

    var shard_results: std.ArrayList([]const u8) = .empty;
    defer {
        for (shard_results.items) |data| allocator.free(data);
        shard_results.deinit(allocator);
    }

    for (dispatchers) |d| {
        const result = d.dispatch(cmd, 0, 0, null) catch continue;
        if (result) |res| {
            switch (res) {
                .ts_floql_result => |qr| {
                    try shard_results.append(allocator, qr.data);
                },
                .err => |e| {
                    return try h.jsonError(allocator, e.message);
                },
                else => {},
            }
        }
    }

    // --- Decode binary SeriesSet results and build JSON ----------------------
    // SeriesSet wire format (per shard):
    //   [series_count: u32]
    //   per series:
    //     [key_len: u32][key]
    //     [field_len: u32][field]
    //     [point_count: u32]
    //     per point:
    //       [timestamp_ms: i64][value: f64]

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("query", query_text);

    var series_arr = try obj.arrayField("series");
    try series_arr.begin();

    for (shard_results.items) |data| {
        var reader = h.BinaryReader.init(data);
        const series_count = reader.readU32() orelse continue;

        var si: u32 = 0;
        while (si < series_count) : (si += 1) {
            // key
            const key_len = reader.readU32() orelse break;
            const key_bytes = reader.readBytes(key_len) orelse break;
            // field
            const field_len = reader.readU32() orelse break;
            const field_bytes = reader.readBytes(field_len) orelse break;
            // point count
            const point_count = reader.readU32() orelse break;

            try series_arr.next();
            var series_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try series_obj.begin();
            try series_obj.stringField("key", key_bytes);
            try series_obj.stringField("field", field_bytes);

            var points_arr = try series_obj.arrayField("points");
            try points_arr.begin();

            var pi: u32 = 0;
            while (pi < point_count) : (pi += 1) {
                const ts_raw = reader.readBytes(8) orelse break;
                const timestamp: i64 = @bitCast(std.mem.readInt(u64, ts_raw[0..8], .little));
                const val_raw = reader.readBytes(8) orelse break;
                const val: f64 = @bitCast(std.mem.readInt(u64, val_raw[0..8], .little));

                // Skip NaN values
                if (std.math.isNan(val)) continue;

                try points_arr.next();
                var pt_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try pt_obj.begin();
                try pt_obj.intField("t", timestamp);
                try pt_obj.floatField("v", val);
                try pt_obj.end();
            }

            try points_arr.end();
            try series_obj.end();
        }
    }

    try series_arr.end();
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Internal Helpers — Read TS metadata from KV
// =============================================================================

/// Read measurement names by scanning `_ts:ml:` marker keys across all shards.
/// Each shard is scanned locally via `scanLocalKVInternal` with cursor continuation
/// to handle shards with more markers than a single page can hold.
/// Returns owned slice of measurement name strings.
fn readMeasurementList(allocator: Allocator, dispatchers: []*Dispatcher, namespace: []const u8) ![][]const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }

    for (dispatchers) |d| {
        // Paginate through this shard's markers with cursor continuation.
        // scanLocalKVInternal returns cursors in ShardWalker format
        // [shard_id:u16][local_cursor]; we strip the shard prefix for
        // local continuation and stop when the cursor advances to the next shard.
        var local_cursor: ?[]u8 = null;
        defer if (local_cursor) |c| allocator.free(c);

        var pages: u32 = 0;
        while (pages < 10000) : (pages += 1) { // safety limit
            const result = d.scanLocalKVInternal(namespace, "_ts:ml:", 1000, local_cursor) catch break;

            switch (result) {
                .kv_scan_result => |scan| {
                    defer allocator.free(scan.data);

                    const entries = parseScanEntries(allocator, scan.data) catch break;
                    defer {
                        for (entries) |entry| {
                            allocator.free(entry.key);
                            allocator.free(entry.value);
                        }
                        allocator.free(entries);
                    }

                    for (entries) |entry| {
                        // Strip _ts:ml: prefix to get measurement name
                        if (entry.key.len > ts_keys.MEASUREMENT_MARKER_PREFIX_LEN) {
                            const name = entry.key[ts_keys.MEASUREMENT_MARKER_PREFIX_LEN..];
                            try names.append(allocator, try allocator.dupe(u8, name));
                        }
                    }

                    // Parse cursor from scan tail for continuation
                    const page = parseScanTailWithVersion(scan.data);

                    if (local_cursor) |c| allocator.free(c);
                    local_cursor = null;

                    if (page.has_more) {
                        if (page.next_cursor) |c| {
                            // Cursor is ShardWalker format: [shard_id:u16][local_key]
                            // Strip the 2-byte shard prefix for local continuation.
                            // If shard advanced (cursor points to next shard), stop
                            // — we'll get that shard when we iterate its dispatcher.
                            if (c.len > 2) {
                                local_cursor = allocator.dupe(u8, c[2..]) catch null;
                            }
                            // If cursor is just [shard_id:u16] with no local part,
                            // this shard is exhausted — break to next dispatcher.
                        }
                    }
                    if (local_cursor == null) break;
                },
                else => break,
            }
        }

        // Reset for next dispatcher
        if (local_cursor) |c| {
            allocator.free(c);
            local_cursor = null;
        }
    }

    return try names.toOwnedSlice(allocator);
}

/// Read `_ts:fields:{measurement}` from KV and decode FieldRegistry.
/// Returns owned slice of field name strings.
fn readFieldRegistry(allocator: Allocator, dispatchers: []*Dispatcher, namespace: []const u8, measurement: []const u8) ![][]const u8 {
    var key_buf: [256]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "_ts:fields:{s}", .{measurement}) catch return error.KeyTooLong;

    const target = h.routeToShard(dispatchers, namespace, key);

    const result = target.dispatch(.{
        .kv_get = .{
            .namespace = namespace,
            .key = key,
            .version = null,
        },
    }, 0, 0, null) catch return error.DispatchFailed;

    if (result) |res| {
        switch (res) {
            .kv_value => |v| {
                defer allocator.free(v.value);
                const fr = FieldRegistry.decode(v.value, allocator) catch {
                    return try allocator.alloc([]const u8, 0);
                };
                return fr.fields; // Transfer ownership
            },
            .kv_not_found => return try allocator.alloc([]const u8, 0),
            else => return try allocator.alloc([]const u8, 0),
        }
    }

    return try allocator.alloc([]const u8, 0);
}

/// Intermediate series info for JSON output
const SeriesInfo = struct {
    canonical_key: []const u8,
    approx_count: u64,
    last_write_ms: i64,
    created_at_ms: i64,
    tags: []TagPair,
};

const TagPair = struct {
    key: []const u8,
    value: []const u8,
};

/// Count series for a measurement by scanning _ts:meta: keys across all shards.
/// Uses scanLocalKVInternal (connection_id=0) to see internal `_ts:` keys.
fn countSeriesForMeasurement(allocator: Allocator, dispatchers: []*Dispatcher, namespace: []const u8, measurement: []const u8) u32 {
    var count: u32 = 0;

    for (dispatchers) |d| {
        const result = d.scanLocalKVInternal(namespace, "_ts:meta:", 10000, null) catch continue;

        switch (result) {
            .kv_scan_result => |scan| {
                defer allocator.free(scan.data);

                // Parse scan entries and check which belong to this measurement
                const entries = parseScanEntries(allocator, scan.data) catch continue;
                defer {
                    for (entries) |entry| {
                        allocator.free(entry.key);
                        allocator.free(entry.value);
                    }
                    allocator.free(entries);
                }

                for (entries) |entry| {
                    // Decode SeriesMetadata to check measurement name
                    var meta = SeriesMetadata.decode(entry.value, allocator) catch continue;
                    defer meta.deinit(allocator);

                    if (std.mem.eql(u8, meta.measurement, measurement)) {
                        count += 1;
                    }
                }
            },
            else => {},
        }
    }

    return count;
}

/// Read series metadata for a measurement across all shards.
fn readSeriesForMeasurement(
    allocator: Allocator,
    dispatchers: []*Dispatcher,
    namespace: []const u8,
    measurement: []const u8,
) ![]SeriesInfo {
    var series: std.ArrayList(SeriesInfo) = .empty;
    errdefer {
        for (series.items) |s| {
            allocator.free(s.canonical_key);
            for (s.tags) |tag| {
                allocator.free(tag.key);
                allocator.free(tag.value);
            }
            allocator.free(s.tags);
        }
        series.deinit(allocator);
    }

    for (dispatchers) |d| {
        const result = d.scanLocalKVInternal(namespace, "_ts:meta:", 1000, null) catch continue;

        switch (result) {
            .kv_scan_result => |scan| {
                defer allocator.free(scan.data);

                const entries = parseScanEntries(allocator, scan.data) catch continue;
                defer {
                    for (entries) |entry| {
                        allocator.free(entry.key);
                        allocator.free(entry.value);
                    }
                    allocator.free(entries);
                }

                for (entries) |entry| {
                    var meta = SeriesMetadata.decode(entry.value, allocator) catch continue;

                    if (!std.mem.eql(u8, meta.measurement, measurement)) {
                        meta.deinit(allocator);
                        continue;
                    }

                    // Convert tags to our TagPair format (already allocated by decode)
                    var tag_pairs = allocator.alloc(TagPair, meta.tags.len) catch {
                        meta.deinit(allocator);
                        continue;
                    };
                    for (meta.tags, 0..) |tag, i| {
                        tag_pairs[i] = .{
                            .key = tag.key,
                            .value = tag.value,
                        };
                    }

                    // Take ownership of canonical_key and tags from meta
                    try series.append(allocator, .{
                        .canonical_key = meta.canonical_key,
                        .approx_count = meta.approx_count,
                        .last_write_ms = meta.last_write_ms,
                        .created_at_ms = meta.created_at_ms,
                        .tags = tag_pairs,
                    });

                    // Free measurement but NOT canonical_key and tags (transferred)
                    allocator.free(meta.measurement);
                    allocator.free(meta.tags); // Free the Tag slice, not the strings
                }
            },
            else => {},
        }
    }

    return try series.toOwnedSlice(allocator);
}

/// Read retention config for a measurement (returns raw string or null).
fn readRetentionConfig(_: Allocator, dispatchers: []*Dispatcher, namespace: []const u8, measurement: []const u8) ?[]const u8 {
    var key_buf: [256]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "_ts:retention:{s}", .{measurement}) catch return null;

    const target = h.routeToShard(dispatchers, namespace, key);

    const result = target.dispatch(.{
        .kv_get = .{
            .namespace = namespace,
            .key = key,
            .version = null,
        },
    }, 0, 0, null) catch return null;

    if (result) |res| {
        switch (res) {
            .kv_value => |v| {
                // Return raw value as string for now
                return v.value; // Caller must free
            },
            else => return null,
        }
    }
    return null;
}

// =============================================================================
// Scan Entry Parser (same pattern as kv.zig)
// =============================================================================

const ScanEntry = struct {
    key: []u8,
    value: []u8,
};

/// Parse binary scan result into key-value entries.
/// Wire format: [entry_count: u32 LE]
///   per entry: [key_len: u16 LE][key][val_len: u32 LE][val][version: u64 LE]
fn parseScanEntries(allocator: Allocator, data: []const u8) ![]ScanEntry {
    if (data.len < 4) return try allocator.alloc(ScanEntry, 0);

    const entry_count = std.mem.readInt(u32, data[0..4], .little);
    var entries = try allocator.alloc(ScanEntry, entry_count);
    var offset: usize = 4;
    var parsed: usize = 0;

    for (0..entry_count) |_| {
        // key_len: u16
        if (data.len < offset + 2) break;
        const key_len = std.mem.readInt(u16, data[offset..][0..2], .little);
        offset += 2;

        // key
        if (data.len < offset + key_len) break;
        const key = try allocator.dupe(u8, data[offset..][0..key_len]);
        offset += key_len;

        // val_len: u32
        if (data.len < offset + 4) {
            allocator.free(key);
            break;
        }
        const val_len = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        // value
        if (data.len < offset + val_len) {
            allocator.free(key);
            break;
        }
        const value = try allocator.dupe(u8, data[offset..][0..val_len]);
        offset += val_len;

        // version: u64 (skip)
        if (data.len < offset + 8) {
            allocator.free(key);
            allocator.free(value);
            break;
        }
        offset += 8;

        entries[parsed] = .{ .key = key, .value = value };
        parsed += 1;
    }

    if (parsed < entry_count) {
        // Shrink to actual count
        const shrunk = try allocator.alloc(ScanEntry, parsed);
        @memcpy(shrunk, entries[0..parsed]);
        for (entries[parsed..]) |_| {} // entries beyond parsed already freed or never allocated
        allocator.free(entries);
        return shrunk;
    }

    return entries;
}

/// Scan tail info (continuation state).
const ScanTailInfo = struct {
    has_more: bool,
    next_cursor: ?[]const u8, // slice into original data; not owned
};

/// Parse has_more and cursor from scan wire data by skipping past all entries.
/// This is for the non-keys-only format where each entry includes [version:u64].
///
/// Wire format: [count:u32] ([key_len:u16][key][val_len:u32][val][version:u64])* [has_more:u8] [cursor_len:u16][cursor]?
fn parseScanTailWithVersion(data: []const u8) ScanTailInfo {
    if (data.len < 4) return .{ .has_more = false, .next_cursor = null };
    const count = std.mem.readInt(u32, data[0..4], .little);
    var offset: usize = 4;

    for (0..count) |_| {
        // key_len + key
        if (offset + 2 > data.len) return .{ .has_more = false, .next_cursor = null };
        const key_len = std.mem.readInt(u16, data[offset..][0..2], .little);
        offset += 2 + key_len;
        // val_len + val
        if (offset + 4 > data.len) return .{ .has_more = false, .next_cursor = null };
        const val_len = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4 + val_len;
        // version: u64
        if (offset + 8 > data.len) return .{ .has_more = false, .next_cursor = null };
        offset += 8;
    }

    // has_more: u8
    if (offset >= data.len) return .{ .has_more = false, .next_cursor = null };
    const has_more = data[offset] != 0;
    offset += 1;
    // cursor_len: u16
    if (offset + 2 > data.len) return .{ .has_more = has_more, .next_cursor = null };
    const cursor_len = std.mem.readInt(u16, data[offset..][0..2], .little);
    offset += 2;
    if (cursor_len == 0 or offset + cursor_len > data.len) return .{ .has_more = has_more, .next_cursor = null };
    return .{ .has_more = has_more, .next_cursor = data[offset..][0..cursor_len] };
}
