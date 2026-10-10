//! A KV key is stored bare in "default", so one holding a NUL could name
//! another namespace's "ns\x00key". Such keys are refused by client requests
//! and pipeline KV sinks, and a kv_lookup built from one finds nothing.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

/// Send one request over the client protocol and return its status and
/// message: a key holding a NUL cannot be passed as a command-line argument.
fn rawCall(ctx: *stdx.testing.TestContext, op: anytype, namespace: []const u8, key: []const u8, value: []const u8, out: []u8) !struct { status: u8, data: []const u8 } {
    const proto = @import("src").protocol.proto;
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(op);
    header.request_id = 7;
    const req: proto.Request = .{ .header = header, .namespace = namespace, .key = key, .value = value, .options = "" };
    var buf: [1024]u8 = undefined;
    const bytes = try req.serialize(&buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    _ = std.c.write(fd, bytes.ptr, bytes.len);
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

fn bKeyUnchanged(ctx: *stdx.testing.TestContext) !bool {
    const got = try ctx.execCapture(&.{ "kv", "get", "k", "-n", "b" });
    return std.mem.indexOf(u8, got, "b-value") != null and std.mem.indexOf(u8, got, "overwrite") == null and std.mem.indexOf(u8, got, "nul-key") == null;
}

fn waitFor(ctx: *stdx.testing.TestContext, args: []const []const u8, expected: []const u8) !bool {
    for (0..60) |_| {
        var r = try ctx.cli.run(args);
        defer r.deinit();
        if (r.stdoutContains(expected)) return true;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return false;
}

test "e2e/kv: a key holding a NUL is refused, and a default listing shows only default's keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const proto = @import("src").protocol.proto;
    try ctx.exec(&.{ "kv", "set", "k", "b-value", "-n", "b" });
    try ctx.exec(&.{ "kv", "set", "mine", "default-value" });

    var out: [512]u8 = undefined;
    for ([_]proto.OpCode{ .kv_put, .kv_get, .kv_delete }) |op| {
        const r = try rawCall(ctx, op, "", "b\x00k", "overwrite", &out);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "key must not contain NUL") != null);
    }
    try testing.expect(try bKeyUnchanged(ctx));

    const listed = try ctx.execCapture(&.{ "kv", "list" });
    try testing.expect(std.mem.indexOf(u8, listed, "mine") != null);
    try testing.expect(std.mem.indexOf(u8, listed, "b-value") == null);
}

test "e2e/kv: a pipeline's KV sink drops a record whose key holds a NUL" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "k", "b-value", "-n", "b" });
    const def =
        \\kind: Processing
        \\name: nul-sink
        \\sources.[0].stream.name: nul-in
        \\operators.[0].type: keyby
        \\operators.[0].name: by-id
        \\operators.[0].key_expression: $.id
        \\sinks.[0].kv.namespace: default
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "nul-sink.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "processing", "submit", path });
    try ctx.exec(&.{ "stream", "append", "nul-in", "{\"id\":\"b\\u0000k\",\"v\":\"nul-key\"}" });
    try ctx.exec(&.{ "stream", "append", "nul-in", "{\"id\":\"control\",\"v\":\"ran\"}" });
    // Appended after the NUL record: once it lands, that record was processed.
    try testing.expect(try waitFor(ctx, &.{ "kv", "get", "control" }, "ran"));
    try testing.expect(try bKeyUnchanged(ctx));
}

test "e2e/kv: a kv_lookup whose key holds a NUL finds nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "k", "b-value", "-n", "b" });
    try ctx.exec(&.{ "kv", "set", "known", "yes" });
    const def =
        \\kind: Processing
        \\name: nul-lookup
        \\sources.[0].stream.name: look-in
        \\sinks.[0].stream.name: look-out
        \\operators.[0].type: kv_lookup
        \\operators.[0].name: check
        \\operators.[0].lookup_key: ${$.id}
        \\operators.[0].mode: filter
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "nul-lookup.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "processing", "submit", path });
    try ctx.exec(&.{ "stream", "append", "look-in", "{\"id\":\"b\\u0000k\",\"tag\":\"nul-key\"}" });
    try ctx.exec(&.{ "stream", "append", "look-in", "{\"id\":\"known\",\"tag\":\"control\"}" });
    try testing.expect(try waitFor(ctx, &.{ "stream", "read", "look-out", "--start", "0-0", "--limit", "100" }, "control"));
    var r = try ctx.cli.run(&.{ "stream", "read", "look-out", "--start", "0-0", "--limit", "100" });
    defer r.deinit();
    try testing.expect(!r.stdoutContains("nul-key"));
}

test "e2e/kv: the dashboard finds no key holding a NUL" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "k", "b-value", "-n", "b" });
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var own = try http.get("/api/v1/kv/namespaces/b/keys/k");
    defer own.deinit();
    try testing.expectEqual(@as(u16, 200), own.status);
    try testing.expect(own.bodyContains("b-value"));

    for ([_][]const u8{ "/api/v1/kv/namespaces/default/keys/b%00k", "/api/v1/kv/namespaces/default/keys/b%00k/history" }) |path| {
        var r = try http.get(path);
        defer r.deinit();
        try testing.expectEqual(@as(u16, 404), r.status);
        try testing.expect(!r.bodyContains("b-value"));
    }
}

test "e2e/stream: a stream name holding a NUL is refused, so one namespace can't name another's stream" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const proto = @import("src").protocol.proto;
    try ctx.exec(&.{ "stream", "create", "orders", "--partitions", "4", "-n", "b" });
    try ctx.exec(&.{ "stream", "append", "orders", "o1", "-n", "b" });
    try ctx.exec(&.{ "stream", "group", "create", "orders", "--group", "audit", "-n", "b" });

    // "default" names are bare elsewhere; in any namespace this spells b's.
    var out: [512]u8 = undefined;
    var one: [4]u8 = undefined;
    std.mem.writeInt(u32, &one, 1, .little);
    for ([_]struct { op: proto.OpCode, value: []const u8 }{
        .{ .op = .stream_create, .value = &one },
        .{ .op = .stream_delete, .value = "" },
    }) |c| {
        const r = try rawCall(ctx, c.op, "a", "b\x00orders", c.value, &out);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "stream name must not contain NUL") != null);
    }

    var info = try ctx.cli.run(&.{ "stream", "info", "orders", "-n", "b" });
    defer info.deinit();
    try testing.expect(info.contains("Partitions: 4"));
    var group = try ctx.cli.run(&.{ "stream", "group", "info", "orders", "--group", "audit", "-n", "b" });
    defer group.deinit();
    try testing.expect(group.contains("audit") and !group.contains("not found"));
    var listed = try ctx.cli.run(&.{ "stream", "list", "-n", "b" });
    defer listed.deinit();
    try testing.expect(listed.contains("orders"));
}
