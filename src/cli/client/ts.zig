//! Time-Series Client Operations
//!
//! TS operations for the Flo CLI client.
//! All functions take a *Client and namespace as parameters.
//!
//! Wire mapping:
//!   OpCode: ts_write / ts_read / ts_query / ts_list / ts_delete / ts_retention
//!   namespace = req.namespace
//!   key = measurement name
//!   value = the payload (for a write, one f64, little-endian)
//!   options = TLV-encoded parameters (tags, time range, window, etc.)

const std = @import("std");
const base = @import("base.zig");
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");

/// Write one value of one field. `field` null means the server's default
/// field ("value"); a null `timestamp_ms` lets the server stamp the point.
pub fn write(
    client: *Client,
    namespace: []const u8,
    measurement: []const u8,
    field: ?[]const u8,
    value: f64,
    tags: []const u8,
    timestamp_ms: ?i64,
) !Response {
    var options_buf: [1024]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (tags.len > 0) {
        builder.addString(.ts_tags, tags) catch return error.OptionsBufferTooSmall;
    }
    if (field) |f| {
        builder.addString(.ts_field, f) catch return error.OptionsBufferTooSmall;
    }
    if (timestamp_ms) |ts| {
        builder.addI64(.ts_timestamp, ts) catch return error.OptionsBufferTooSmall;
    }

    var value_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &value_buf, @bitCast(value), .little);
    return client.sendRequestWithOptions(.ts_write, namespace, measurement, &value_buf, builder.getOptions());
}

/// Read raw data points for a measurement
pub fn read(
    client: *Client,
    namespace: []const u8,
    measurement: []const u8,
    opts: ReadOptions,
) !Response {
    var options_buf: [256]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (opts.tags.len > 0) {
        builder.addString(.ts_tags, opts.tags) catch return error.OptionsBufferTooSmall;
    }

    if (opts.field.len > 0) {
        builder.addString(.ts_field, opts.field) catch return error.OptionsBufferTooSmall;
    }

    if (opts.from_ms != 0) {
        builder.addI64(.ts_from_ms, opts.from_ms) catch return error.OptionsBufferTooSmall;
    }

    if (opts.to_ms != 0) {
        builder.addI64(.ts_to_ms, opts.to_ms) catch return error.OptionsBufferTooSmall;
    }

    if (opts.limit != 10000) {
        builder.addU32(.limit, @intCast(opts.limit)) catch return error.OptionsBufferTooSmall;
    }

    return client.sendRequestWithOptions(.ts_read, namespace, measurement, "", builder.getOptions());
}

pub const ReadOptions = struct {
    tags: []const u8 = "",
    field: []const u8 = "",
    from_ms: i64 = 0,
    to_ms: i64 = 0,
    limit: u32 = 10000,
};

/// Query with aggregation
pub fn query(
    client: *Client,
    namespace: []const u8,
    measurement: []const u8,
    opts: QueryOptions,
) !Response {
    var options_buf: [256]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (opts.tags.len > 0) {
        builder.addString(.ts_tags, opts.tags) catch return error.OptionsBufferTooSmall;
    }

    if (opts.field.len > 0) {
        builder.addString(.ts_field, opts.field) catch return error.OptionsBufferTooSmall;
    }

    if (opts.from_ms != 0) {
        builder.addI64(.ts_from_ms, opts.from_ms) catch return error.OptionsBufferTooSmall;
    }

    if (opts.to_ms != 0) {
        builder.addI64(.ts_to_ms, opts.to_ms) catch return error.OptionsBufferTooSmall;
    }

    if (opts.window_ms != 60000) {
        builder.addI64(.ts_window_ms, opts.window_ms) catch return error.OptionsBufferTooSmall;
    }

    if (!std.mem.eql(u8, opts.aggregation, "avg")) {
        builder.addString(.ts_aggregation, opts.aggregation) catch return error.OptionsBufferTooSmall;
    }

    return client.sendRequestWithOptions(.ts_query, namespace, measurement, "", builder.getOptions());
}

pub const QueryOptions = struct {
    tags: []const u8 = "",
    field: []const u8 = "",
    from_ms: i64 = 0,
    to_ms: i64 = 0,
    window_ms: i64 = 60000,
    aggregation: []const u8 = "avg",
};

/// List measurements or series for a measurement.
/// Server returns a structured binary page; pass an opaque cursor from a
/// previous response to fetch the next page.
pub fn list(
    client: *Client,
    namespace: []const u8,
    measurement: []const u8,
    limit: ?u32,
    cursor: ?[]const u8,
) !Response {
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (limit) |l| {
        builder.addU32(.limit, l) catch return error.OptionsBufferTooSmall;
    }

    const options = builder.getOptions();
    return client.sendRequestWithOptions(.ts_list, namespace, measurement, cursor orelse "", options);
}

/// Delete a series
pub fn delete(
    client: *Client,
    namespace: []const u8,
    measurement: []const u8,
    tags: []const u8,
) !Response {
    if (tags.len > 0) {
        var options_buf: [256]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        builder.addString(.ts_tags, tags) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.ts_delete, namespace, measurement, "", builder.getOptions());
    }
    return client.sendRequest(.ts_delete, namespace, measurement, "");
}

/// Delete a measurement's points older than `raw_ttl`, once.
pub fn retention(
    client: *Client,
    namespace: []const u8,
    measurement: []const u8,
    raw_ttl: []const u8,
) !Response {
    var options_buf: [512]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    builder.addString(.ts_raw_ttl, raw_ttl) catch return error.OptionsBufferTooSmall;

    return client.sendRequestWithOptions(.ts_retention, namespace, measurement, "", builder.getOptions());
}

/// Execute a FloQL query
pub fn floql(
    client: *Client,
    namespace: []const u8,
    query_string: []const u8,
) !Response {
    // key is empty (FloQL doesn't target a single measurement via key),
    // value carries the query string. OpCode ts_floql.
    return client.sendRequest(.ts_floql, namespace, "", query_string);
}
