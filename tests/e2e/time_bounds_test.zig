//! Times a client supplies — TTLs, ages, range bounds, FloQL durations — that
//! don't fit nanoseconds: a TTL or duration is refused, a lower bound past the
//! clock matches nothing, and an upper bound past it is no bound.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

const Reply = struct { status: u8, data: []const u8 };

/// Sends a hand-built request and returns the status: the CLI exits 0 on a
/// refused request, so its exit says nothing.
fn rawCall(ctx: *stdx.testing.TestContext, op: proto.OpCode, key: []const u8, value: []const u8, options: []const u8, out: []u8) !Reply {
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(op);
    header.request_id = 7;
    const req: proto.Request = .{ .header = header, .namespace = "default", .key = key, .value = value, .options = options };
    var buf: [1024]u8 = undefined;
    const bytes = try req.serialize(&buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    if (std.c.write(fd, bytes.ptr, bytes.len) != @as(isize, @intCast(bytes.len))) return error.ShortWrite;
    var got: usize = 0;
    var waited: u32 = 0;
    while (waited < 300) : (waited += 1) {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break;
        if (proto.Response.parse(out[0..got])) |resp| return .{ .status = resp.header.status, .data = resp.data } else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NoResponse;
}

fn option(buf: []u8, tag: proto.OptionTag, comptime T: type, value: T) ![]const u8 {
    var b = proto.OptionsBuilder.init(buf);
    if (T == u64) try b.addU64(tag, value) else try b.addI64(tag, value);
    return b.getOptions();
}

fn expectRefused(r: Reply, why: []const u8) !void {
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, why) != null);
}

/// The stored value from a raw kv_get answer: [version:u64][value].
fn kvValue(ctx: *stdx.testing.TestContext, key: []const u8, out: []u8) ![]const u8 {
    const r = try rawCall(ctx, .kv_get, key, "", "", out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), r.status);
    return r.data[8..];
}

/// The point or series count leading a ts read or query answer.
fn leadingCount(r: Reply) !u32 {
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), r.status);
    return std.mem.readInt(u32, r.data[0..4], .little);
}

test "e2e/time: a TTL too large to represent is refused, and the key keeps its value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "k", "v" });
    try ctx.exec(&.{ "kv", "set", "doc", "{\"a\":1}" });

    var out: [4096]u8 = undefined;
    var obuf: [32]u8 = undefined;
    const huge = try option(&obuf, .ttl_ms, u64, std.math.maxInt(u64));
    try expectRefused(try rawCall(ctx, .kv_put, "k", "v2", huge, &out), "ttl too large");
    var ttl: [8]u8 = undefined;
    std.mem.writeInt(u64, &ttl, std.math.maxInt(u64), .little);
    try expectRefused(try rawCall(ctx, .kv_touch, "k", &ttl, "", &out), "ttl too large");
    // [path_len][path][json]
    const set = [_]u8{ 3, 0 } ++ "$.a".* ++ "2".*;
    try expectRefused(try rawCall(ctx, .kv_json_set, "doc", &set, huge, &out), "ttl too large");

    try testing.expectEqualStrings("v", try kvValue(ctx, "k", &out));
    try testing.expectEqualStrings("{\"a\":1}", try kvValue(ctx, "doc", &out));
}

test "e2e/time: a TTL option that isn't 8 bytes is refused, and the key keeps its value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "k", "v" });
    try ctx.exec(&.{ "kv", "set", "doc", "{\"a\":1}" });

    var out: [4096]u8 = undefined;
    // [tag][len][u32 ms]: read as no TTL, the key would never expire.
    const short = [_]u8{ @intFromEnum(proto.OptionTag.ttl_ms), 4, 0xe8, 0x03, 0, 0 };
    try expectRefused(try rawCall(ctx, .kv_put, "k", "v2", &short, &out), "ttl_ms must be 8 bytes");
    const set = [_]u8{ 3, 0 } ++ "$.a".* ++ "2".*;
    try expectRefused(try rawCall(ctx, .kv_json_set, "doc", &set, &short, &out), "ttl_ms must be 8 bytes");

    // A put inside a transaction is buffered, not proposed: it is checked
    // before the buffer, and the commit writes nothing.
    const begin = try rawCall(ctx, .kv_begin_txn, "k", "", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), begin.status);
    const txn_id = std.mem.readInt(u64, begin.data[1..9], .little);
    var tbuf: [32]u8 = undefined;
    var tb = proto.OptionsBuilder.init(&tbuf);
    try tb.addU64(.txn_id, txn_id);
    var in_txn: [64]u8 = undefined;
    @memcpy(in_txn[0..short.len], &short);
    @memcpy(in_txn[short.len..][0..tb.getOptions().len], tb.getOptions());
    try expectRefused(try rawCall(ctx, .kv_put, "k", "v3", in_txn[0 .. short.len + tb.getOptions().len], &out), "ttl_ms must be 8 bytes");
    const commit = try rawCall(ctx, .kv_commit_txn, "k", "", tb.getOptions(), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), commit.status);

    try testing.expectEqualStrings("v", try kvValue(ctx, "k", &out));
    try testing.expectEqualStrings("{\"a\":1}", try kvValue(ctx, "doc", &out));
}

test "e2e/time: the dashboard's KV put takes ttl_ms in milliseconds and refuses anything else" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    const path = "/api/v1/kv/namespaces/default/keys/k";

    for ([_][2][]const u8{
        .{ "{\"value\":\"v\",\"ttl_seconds\":60}", "unknown field \\\"ttl_seconds\\\"" },
        .{ "{\"value\":\"v\",\"ttl_ms\":1.5}", "ttl_ms must be a whole number of milliseconds" },
        .{ "{\"value\":\"v\",\"ttl_ms\":\"60s\"}", "ttl_ms must be a whole number of milliseconds" },
        .{ "{\"value\":\"v\",\"ttl_ms\":-1}", "ttl_ms must not be negative" },
    }) |case| {
        var resp = try http.put(path, case[0]);
        defer resp.deinit();
        try testing.expect(resp.bodyContains(case[1]));
    }
    var absent = try ctx.cli.run(&.{ "kv", "get", "k", "--output", "table" });
    defer absent.deinit();
    try stdx.testing.assertContains(absent, "(nil)");

    // The read reports an absolute expiry, not a duration a client could
    // send back as ttl_ms.
    var ok = try http.put(path, "{\"value\":\"v\",\"ttl_ms\":800}");
    defer ok.deinit();
    try testing.expectEqual(@as(u16, 200), ok.status);
    var got = try http.get(path);
    defer got.deinit();
    try testing.expect(got.bodyContains("\"expires_at_ms\":"));
    try testing.expect(!got.bodyContains("\"expires_at_ms\":null"));
    stdx.time.sleep(2000 * std.time.ns_per_ms);
    var gone = try ctx.cli.run(&.{ "kv", "get", "k", "--output", "table" });
    defer gone.deinit();
    try stdx.testing.assertContains(gone, "(nil)");
}

/// 90 years: older than the clock, and under the 100-year limit on ages.
const AGE_PAST_CLOCK_S: u64 = 90 * 365 * 24 * 3600;

test "e2e/time: a lower bound past the clock matches nothing, and an age past it trims nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "kept" });
    try ctx.exec(&.{ "ts", "write", "m", "--value", "1" });

    var out: [4096]u8 = undefined;
    var obuf: [32]u8 = undefined;
    const trim = try rawCall(ctx, .stream_trim, "s", "", try option(&obuf, .max_age_seconds, u64, AGE_PAST_CLOCK_S), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), trim.status);
    // An age no clock could reach is a mistake, refused rather than saturated.
    const absurd = try rawCall(ctx, .stream_trim, "s", "", try option(&obuf, .max_age_seconds, u64, std.math.maxInt(u64)), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), absurd.status);

    // The point is there from zero; from past the clock, nothing is.
    try testing.expectEqual(@as(u32, 1), try leadingCount(try rawCall(ctx, .ts_read, "m", "", try option(&obuf, .ts_from_ms, i64, 1), &out)));
    try testing.expectEqual(@as(u32, 0), try leadingCount(try rawCall(ctx, .ts_read, "m", "", try option(&obuf, .ts_from_ms, i64, std.math.maxInt(i64)), &out)));
    try testing.expectEqual(@as(u32, 0), try leadingCount(try rawCall(ctx, .ts_query, "m", "", try option(&obuf, .ts_from_ms, i64, std.math.maxInt(i64)), &out)));
    const query = try rawCall(ctx, .ts_floql, "", "m[20000000000000]", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), query.status);

    var r = try ctx.cli.run(&.{ "stream", "read", "s", "--start", "0-0" });
    defer r.deinit();
    try testing.expect(r.stdoutContains("kept"));
}

test "e2e/time: the dashboard's time-series reads take bounds past the clock" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try ctx.exec(&.{ "ts", "write", "m", "--value", "1" });
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for ([_][]const u8{
        "/api/v1/timeseries/floql?q=m%5B20000000000000%5D",
        "/api/v1/timeseries/m/data?from=20000000000000",
        "/api/v1/timeseries/m/data?window=18446744073709551615",
    }) |path| {
        var resp = try http.get(path);
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
    }
    var health = try http.get("/health");
    defer health.deinit();
    try testing.expectEqual(@as(u16, 200), health.status);
}

test "e2e/time: a FloQL duration too large to represent is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "ts", "write", "m", "--value", "1" });

    var out: [4096]u8 = undefined;
    const query = try rawCall(ctx, .ts_floql, "", "m[106751991168d]", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), query.status);
    try expectRefused(try rawCall(ctx, .ts_retention, "m", "106751991168d", "", &out), "invalid retention duration");

    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "alive" }), "yes") != null);
}

test "e2e/time: a stream retention age past the clock trims nothing when the sweeper runs" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var out: [4096]u8 = undefined;
    var obuf: [32]u8 = undefined;
    var partitions: [4]u8 = undefined;
    std.mem.writeInt(u32, &partitions, 1, .little);
    const absurd = try rawCall(ctx, .stream_create, "aged", &partitions, try option(&obuf, .retention_age, u64, std.math.maxInt(u64)), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), absurd.status);
    const created = try rawCall(ctx, .stream_create, "aged", &partitions, try option(&obuf, .retention_age, u64, AGE_PAST_CLOCK_S), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), created.status);
    try ctx.exec(&.{ "stream", "append", "aged", "kept" });

    // The sweeper runs every 10 s.
    stdx.time.sleep(11 * std.time.ns_per_s);
    var r = try ctx.cli.run(&.{ "stream", "read", "aged", "--start", "0-0" });
    defer r.deinit();
    try testing.expect(r.stdoutContains("kept"));
}
