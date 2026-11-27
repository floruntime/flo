//! A KV key holding a NUL is refused by client requests and pipeline KV
//! sinks, and a kv_lookup built from one finds nothing. A stored key leads
//! with its namespace's fixed-width id, so a NUL could name no other
//! namespace's key; the refusal stands because the CLI and SDKs have always
//! refused such a key, so no client holds one to read back.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const kv_ops = @import("src").protocol.kv_ops;

/// Send one request over the client protocol and return its status and
/// message: a key holding a NUL cannot be passed as a command-line argument.
fn rawCall(ctx: *stdx.testing.TestContext, op: anytype, namespace: []const u8, key: []const u8, value: []const u8, out: []u8) !struct { status: u8, data: []const u8 } {
    const proto = @import("src").protocol.proto;
    const header = proto.RequestHeader.init(op, 7);
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
        if (proto.Response.parse(out[0..got])) |resp| return .{ .status = resp.header.status, .data = if (resp.header.status == 0) resp.data else (try resp.refused()).message } else |_| {}
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
    var body_buf: [32]u8 = undefined;
    for ([_]proto.OpCode{ .kv_put, .kv_get, .kv_delete }) |op| {
        const value = switch (op) {
            .kv_put => try kv_ops.plainSetBody(&body_buf, "overwrite"),
            .kv_get => try kv_ops.plainGetBody(&body_buf),
            // kv.delete: no cas_version or txn.
            else => &[_]u8{ 0, 0 },
        };
        const r = try rawCall(ctx, op, "", "b\x00k", value, &out);
        try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "key must not contain NUL") != null);
    }
    try testing.expect(try bKeyUnchanged(ctx));

    const listed = try ctx.execCapture(&.{ "kv", "scan" });
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
        try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "stream name must not contain control characters") != null);
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

test "e2e/names: a name holding a control character is refused, saying so, and nothing is made" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const proto = @import("src").protocol.proto;
    var out: [512]u8 = undefined;
    for ([_]struct { op: proto.OpCode, key: []const u8, value: []const u8 }{
        .{ .op = .stream_append, .key = "orders\nx", .value = "o1" },
        .{ .op = .stream_group_read, .key = "orders", .value = "\x02\x00g\x1b" },
        .{ .op = .queue_enqueue, .key = "jobs\tx", .value = "m" },
        .{ .op = .action_register, .key = "send\x7f", .value = "" },
        .{ .op = .worker_register, .key = "w\rx", .value = "" },
    }) |c| {
        const r = try rawCall(ctx, c.op, "", c.key, c.value, &out);
        if (std.mem.indexOf(u8, r.data, "must not contain control characters") == null) std.debug.print("{s}: {d} {s}\n", .{ @tagName(c.op), r.status, r.data });
        try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "must not contain control characters") != null);
    }
    // A workflow, and a processing job's stream, by the CLI's own path.
    for ([_]struct { args: []const []const u8, def: []const u8, file: []const u8 }{
        .{ .args = &.{ "workflow", "create", "-f" }, .def = "{\"kind\":\"Workflow\",\"name\":\"wf\\u0009x\",\"version\":\"1.0.0\",\"start\":{\"wait_for_signal\":{\"type\":\"go\"},\"transitions\":{\"success\":\"flo.Completed\"}}}", .file = "e2e-name-wf.yaml" },
        .{ .args = &.{ "processing", "submit" }, .def = "{\"kind\":\"Processing\",\"name\":\"j\",\"sources\":[{\"stream\":{\"name\":\"in\\u000ax\"}}],\"sinks\":[{\"stream\":{\"name\":\"out\"}}]}", .file = "e2e-name-job.yaml" },
    }) |c| {
        const path = try stdx.testing.writeTempYaml(testing.allocator, c.def, c.file);
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        var argv: [4][]const u8 = undefined;
        for (c.args, 0..) |a, i| argv[i] = a;
        argv[c.args.len] = path;
        var r = try ctx.cli.run(argv[0 .. c.args.len + 1]);
        defer r.deinit();
        if (!r.contains("must not contain control characters")) std.debug.print("{s}: {s} {s}\n", .{ c.args[0], r.stdout, r.stderr });
        try testing.expect(r.contains("must not contain control characters"));
    }
    // Nothing made: no stream, queue, action or workflow by those names.
    const streams = try ctx.execCapture(&.{ "stream", "list" });
    try testing.expect(std.mem.indexOf(u8, streams, "orders") == null);
    const actions = try ctx.execCapture(&.{ "action", "list" });
    try testing.expect(std.mem.indexOf(u8, actions, "send") == null);
}

test "e2e/names: every name a request carries refuses control characters: consumers, task types, signal types, TS tag keys and a job's TS names" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const says = "must not contain control characters";
    try ctx.exec(&.{ "stream", "append", "orders", "o1" });
    try ctx.exec(&.{ "queue", "enqueue", "jobs", "m" });
    try ctx.exec(&.{ "action", "register", "send" });
    // A run waiting for a signal, for one sent with a control type.
    const waits = "{\"kind\":\"Workflow\",\"name\":\"waits\",\"version\":\"1.0.0\",\"start\":{\"wait_for_signal\":{\"type\":\"go\"},\"transitions\":{\"success\":\"flo.Completed\"}}}";
    const wpath = try stdx.testing.writeTempYaml(testing.allocator, waits, "e2e-name-waits.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, wpath);
    try ctx.exec(&.{ "workflow", "create", "-f", wpath });
    try ctx.exec(&.{ "workflow", "start", "waits", "{}", "--run-id", "w1" });
    for ([_][]const []const u8{
        &.{ "stream", "group", "read", "or\nders", "--group", "g", "--consumer", "c" },
        &.{ "stream", "group", "read", "orders", "--group", "g", "--consumer", "c\x1b" },
        &.{ "queue", "dequeue", "jobs", "--consumer", "c\tx" },
        &.{ "queue", "dequeue", "jo\tbs" },
        &.{ "queue", "purge", "jo\tbs" },
        &.{ "queue", "dlq", "list", "jo\tbs" },
        &.{ "action", "invoke", "se\x1bnd", "{}" },
        &.{ "worker", "register", "w1", "ty\tpe" },
        &.{ "worker", "await", "se\x7fnd", "--worker-id", "w2", "--block", "0" },
        &.{ "workflow", "signal", "w1", "--type", "g\to" },
        &.{ "ts", "write", "cpu", "--tags", "ho\x1bst=a", "--value", "1" },
    }) |args| {
        var r = try ctx.cli.run(args);
        defer r.deinit();
        if (!r.contains(says)) std.debug.print("{s} {s}: {s} {s}\n", .{ args[0], args[1], r.stdout, r.stderr });
        try testing.expect(r.contains(says));
    }
    // Definitions: a workflow's trigger stream and signal types, a job's
    // name and its TS sink's field.
    for ([_]struct { args: []const []const u8, def: []const u8, file: []const u8 }{
        .{ .args = &.{ "workflow", "create", "-f" }, .def = "{\"kind\":\"Workflow\",\"name\":\"trig\",\"version\":\"1.0.0\",\"trigger\":{\"stream\":\"in\\u0009x\"},\"start\":{\"wait_for_signal\":{\"type\":\"go\"},\"transitions\":{\"success\":\"flo.Completed\"}}}", .file = "e2e-name-trig.yaml" },
        .{ .args = &.{ "workflow", "create", "-f" }, .def = "{\"kind\":\"Workflow\",\"name\":\"sig\",\"version\":\"1.0.0\",\"start\":{\"wait_for_signal\":{\"type\":\"g\\u000ao\"},\"transitions\":{\"success\":\"flo.Completed\"}}}", .file = "e2e-name-sig.yaml" },
        .{ .args = &.{ "processing", "submit" }, .def = "{\"kind\":\"Processing\",\"name\":\"j\\u0009x\",\"sources\":[{\"stream\":{\"name\":\"in\"}}],\"sinks\":[{\"stream\":{\"name\":\"out\"}}]}", .file = "e2e-name-jobname.yaml" },
        .{ .args = &.{ "processing", "submit" }, .def = "{\"kind\":\"Processing\",\"name\":\"tsj\",\"sources\":[{\"stream\":{\"name\":\"in\"}}],\"sinks\":[{\"ts\":{\"measurement\":\"m\",\"fields\":{\"lo\\u0009ad\":\"l\"}}}]}", .file = "e2e-name-tsfield.yaml" },
    }) |c| {
        const path = try stdx.testing.writeTempYaml(testing.allocator, c.def, c.file);
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        var argv: [4][]const u8 = undefined;
        for (c.args, 0..) |a, i| argv[i] = a;
        argv[c.args.len] = path;
        var r = try ctx.cli.run(argv[0 .. c.args.len + 1]);
        defer r.deinit();
        if (!r.contains(says)) std.debug.print("{s}: {s} {s}\n", .{ c.file, r.stdout, r.stderr });
        try testing.expect(r.contains(says));
    }
}

test "e2e/names: the CLI keys a workflow create by its definition's own name, however its text runs" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    // Stored under the parsed name, not one taken from the file name.
    const proto = @import("src").protocol.proto;
    var out: [4096]u8 = undefined;
    for ([_]struct { def: []const u8, file: []const u8, stem: []const u8, name: []const u8 }{
        // A nested "name" comes first in the text.
        .{ .def = "{\"kind\":\"Workflow\",\"plans\":{\"p\":{\"executors\":[{\"name\":\"e1\",\"run\":\"@actions/send\"}]}},\"name\":\"nested\",\"version\":\"1.0.0\",\"start\":{\"run\":\"@plan/p\",\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}", .file = "e2e-file-one.yaml", .stem = "e2e-file-one", .name = "nested" },
        // A comment after the name.
        .{ .def = "kind: Workflow\nname: commented  # the name\nversion: 1.0.0\nstart:\n  wait_for_signal:\n    type: go\n  transitions:\n    success: flo.Completed\n", .file = "e2e-file-two.yaml", .stem = "e2e-file-two", .name = "commented" },
    }) |c| {
        const path = try stdx.testing.writeTempYaml(testing.allocator, c.def, c.file);
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        var r = try ctx.cli.run(&.{ "workflow", "create", "-f", path });
        defer r.deinit();
        var want: [64]u8 = undefined;
        const says = try std.fmt.bufPrint(&want, "Created workflow: {s}\n", .{c.name});
        if (!std.mem.eql(u8, r.stdout, says)) std.debug.print("{s}: {s} {s}\n", .{ c.file, r.stdout, r.stderr });
        try testing.expectEqualStrings(says, r.stdout);
        try testing.expectEqual(@as(u8, 0), (try rawCall(ctx, proto.OpCode.workflow_get_definition, "", c.name, "", &out)).status);
        try testing.expectEqual(@backingInt(proto.StatusCode.not_found), (try rawCall(ctx, proto.OpCode.workflow_get_definition, "", c.stem, "", &out)).status);
    }
}

test "e2e/names: a workflow create or signal the server must refuse is refused by status: a key unlike the definition's name, a control byte in a trigger stream or a signal type" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const proto = @import("src").protocol.proto;
    const bad = @backingInt(proto.StatusCode.bad_request);
    var out: [512]u8 = undefined;

    // Key "a", definition named "b": refused, and neither name is stored.
    const b = "{\"kind\":\"Workflow\",\"name\":\"b\",\"version\":\"1.0.0\",\"start\":{\"wait_for_signal\":{\"type\":\"go\"},\"transitions\":{\"success\":\"flo.Completed\"}}}";
    for ([_][]const u8{ "a", "b\n" }) |key| {
        const r = try rawCall(ctx, proto.OpCode.workflow_create, "", key, b, &out);
        try testing.expectEqual(bad, r.status);
        try testing.expectEqualStrings("bad request: the request's workflow name and its definition's name differ", r.data);
    }
    for ([_][]const u8{ "a", "b" }) |name| {
        const r = try rawCall(ctx, proto.OpCode.workflow_get_definition, "", name, "", &out);
        try testing.expectEqual(@backingInt(proto.StatusCode.not_found), r.status);
    }

    // A control byte in a trigger stream, or in a wait_for_signal type.
    for ([_]struct { name: []const u8, def: []const u8, says: []const u8 }{
        .{ .name = "trig", .def = "{\"kind\":\"Workflow\",\"name\":\"trig\",\"version\":\"1.0.0\",\"trigger\":{\"stream\":\"in\\u0009x\"},\"start\":{\"wait_for_signal\":{\"type\":\"go\"},\"transitions\":{\"success\":\"flo.Completed\"}}}", .says = "a stream name" },
        .{ .name = "sig", .def = "{\"kind\":\"Workflow\",\"name\":\"sig\",\"version\":\"1.0.0\",\"start\":{\"wait_for_signal\":{\"type\":\"g\\u000ao\"},\"transitions\":{\"success\":\"flo.Completed\"}}}", .says = "a signal type" },
    }) |c| {
        const r = try rawCall(ctx, proto.OpCode.workflow_create, "", c.name, c.def, &out);
        try testing.expectEqual(bad, r.status);
        var want: [128]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "bad request: {s} must not contain control characters (bytes 0x00-0x1f or 0x7f)", .{c.says}), r.data);
        const got = try rawCall(ctx, proto.OpCode.workflow_get_definition, "", c.name, "", &out);
        try testing.expectEqual(@backingInt(proto.StatusCode.not_found), got.status);
    }

    // A signal with a control type, to a run that waits: refused, and the
    // run still waits for its own signal.
    const wpath = try stdx.testing.writeTempYaml(testing.allocator, b, "e2e-name-raw-b.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, wpath);
    try ctx.exec(&.{ "workflow", "create", "-f", wpath });
    try ctx.exec(&.{ "workflow", "start", "b", "{}", "--run-id", "rb1" });
    var sig: [8]u8 = undefined;
    std.mem.writeInt(u16, sig[0..2], 3, .little);
    @memcpy(sig[2..5], "g\x1bo");
    const r = try rawCall(ctx, proto.OpCode.workflow_signal, "", "rb1", sig[0..5], &out);
    try testing.expectEqual(bad, r.status);
    try testing.expectEqualStrings("bad request: a signal type must not contain control characters (bytes 0x00-0x1f or 0x7f)", r.data);
    try testing.expect(try waitFor(ctx, &.{ "workflow", "status", "rb1" }, "waiting"));
}
