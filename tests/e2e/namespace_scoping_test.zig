//! Whatever is reached by id — a queue message by seq, an action run, a job,
//! a transaction — is reached only from its own namespace, and an action
//! named in two namespaces is two actions. A pipeline reads where it likes
//! and writes only into its own namespace.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

fn after(output: []const u8, prefix: []const u8) ?[]const u8 {
    const start = (std.mem.indexOf(u8, output, prefix) orelse return null) + prefix.len;
    var end = start;
    while (end < output.len and output[end] != '\n' and output[end] != '\r' and output[end] != ' ' and output[end] != ',' and output[end] != '}') end += 1;
    return if (end > start) output[start..end] else null;
}

/// The seq of the first message in `queue`, as the dashboard lists it.
fn firstSeq(ctx: *stdx.testing.TestContext, queue: []const u8, ns: []const u8, buf: []u8) ![]const u8 {
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/api/v1/queues/{s}/messages?namespace={s}", .{ queue, ns });
    var waited: u32 = 0;
    while (waited < 30) : (waited += 1) {
        var r = try http.get(path);
        defer r.deinit();
        if (after(r.body, "\"seq\":")) |seq| {
            @memcpy(buf[0..seq.len], seq);
            return buf[0..seq.len];
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return error.NoMessage;
}

test "e2e/scoping: a queue message is acked or nacked only from its own queue and namespace" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try ctx.exec(&.{ "queue", "enqueue", "q", "secret", "-n", "qa" });
    var seq_buf: [24]u8 = undefined;
    const seq = try firstSeq(ctx, "q", "qa", &seq_buf);

    for ([_][]const u8{ "ack", "nack" }) |verb| {
        var r = try ctx.cli.run(&.{ "queue", verb, "q", seq, "-n", "qb" });
        defer r.deinit();
        try stdx.testing.assertContains(r, "message not found in this queue");
        var other_queue = try ctx.cli.run(&.{ "queue", verb, "other", seq, "-n", "qa" });
        defer other_queue.deinit();
        try stdx.testing.assertContains(other_queue, "message not found in this queue");
    }
    // The message is still there for its own queue, which can ack it.
    var ack = try ctx.cli.run(&.{ "queue", "ack", "q", seq, "-n", "qa" });
    defer ack.deinit();
    try stdx.testing.assertStdoutContains(ack, "OK");
    const left = try ctx.execCapture(&.{ "queue", "dequeue", "q", "-n", "qa", "--timeout", "100" });
    try testing.expect(std.mem.indexOf(u8, left, "(no messages)") != null);
}

test "e2e/scoping: an action named in two namespaces is two actions" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "act", "-n", "a" });
    try ctx.exec(&.{ "action", "register", "act", "-n", "b" });
    try ctx.exec(&.{ "action", "delete", "act", "-n", "b" });
    const list = try ctx.execCapture(&.{ "action", "list", "-n", "a" });
    try testing.expect(std.mem.indexOf(u8, list, "act") != null);
    const invoked = try ctx.execCapture(&.{ "action", "invoke", "act", "{}", "-n", "a" });
    try testing.expect(after(invoked, "Result: ") != null);
    var gone = try ctx.cli.run(&.{ "action", "invoke", "act", "{}", "-n", "b" });
    defer gone.deinit();
    try stdx.testing.assertContains(gone, "action not found");
}

test "e2e/scoping: a run is seen and claimed only from its own namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "act", "-n", "a" });
    try ctx.exec(&.{ "action", "register", "act", "-n", "b" });
    try ctx.exec(&.{ "worker", "register", "wa", "act", "-n", "a" });
    try ctx.exec(&.{ "worker", "register", "wb", "act", "-n", "b" });
    const invoked = try ctx.execCapture(&.{ "action", "invoke", "act", "{}", "-n", "a" });
    const run_id = after(invoked, "Result: ") orelse return error.NoRunId;

    var status = try ctx.cli.run(&.{ "action", "status", run_id, "-n", "b" });
    defer status.deinit();
    try stdx.testing.assertContains(status, "Run not found");

    var other = try ctx.cli.run(&.{ "worker", "await", "act", "--worker-id", "wb", "--block", "500", "-n", "b" });
    defer other.deinit();
    try stdx.testing.assertStdoutContains(other, "(no tasks)");

    var own = try ctx.cli.run(&.{ "worker", "await", "act", "--worker-id", "wa", "--block", "3000", "-n", "a" });
    defer own.deinit();
    try stdx.testing.assertContains(own, run_id);
}

test "e2e/scoping: a job is seen and changed only from its own namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const def =
        \\kind: Processing
        \\name: scoped-job
        \\sources.[0].stream.name: in
        \\sinks.[0].stream.name: out
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "scoped-job.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    const submitted = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "a" });
    const job_id = after(submitted, "Job submitted: ") orelse return error.NoJobId;

    for ([_][]const u8{ "status", "stop", "cancel" }) |verb| {
        var r = try ctx.cli.run(&.{ "processing", verb, job_id, "-n", "b" });
        defer r.deinit();
        try stdx.testing.assertContains(r, "Job not found");
    }
    var own = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "a" });
    defer own.deinit();
    try stdx.testing.assertContains(own, "RUNNING");
}

test "e2e/scoping: a transaction is committed or rolled back only from its own namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const begun = try ctx.execCapture(&.{ "kv", "begin", "--routing-key", "k", "-n", "a" });
    const tid = after(begun, "txn_id=") orelse return error.NoTxnId;
    for ([_][]const u8{ "commit", "rollback" }) |verb| {
        var r = try ctx.cli.run(&.{ "kv", verb, tid, "--routing-key", "k", "-n", "b" });
        defer r.deinit();
        try stdx.testing.assertContains(r, "transaction not found");
    }
    // A write in it from elsewhere learns no more: not found.
    var put = try ctx.cli.run(&.{ "kv", "set", "k", "v", "--routing-key", "k", "--txn", tid, "-n", "b" });
    defer put.deinit();
    try stdx.testing.assertContains(put, "transaction not found");
    var own = try ctx.cli.run(&.{ "kv", "rollback", tid, "--routing-key", "k", "-n", "a" });
    defer own.deinit();
    try stdx.testing.assertStdoutContains(own, "OK");
}

test "e2e/scoping: a workflow runs the action in its own namespace, not one elsewhere" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    // Only "default" has the action; the workflow lives in "w".
    try ctx.exec(&.{ "action", "register", "wf-act" });
    try ctx.exec(&.{ "worker", "register", "wd", "wf-act" });
    const yaml =
        \\kind: Workflow
        \\name: scoped-wf
        \\version: "1.0.0"
        \\start:
        \\  run: "@actions/wf-act"
        \\  transitions:
        \\    success: flo.Completed
        \\    failure: flo.Failed
    ;
    const path = try stdx.testing.writeTempYaml(testing.allocator, yaml, "scoped-wf.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "workflow", "create", "-f", path, "-n", "w" });
    try ctx.exec(&.{ "workflow", "start", "scoped-wf", "{}", "-n", "w" });
    // No run of default's action was started for it.
    var none = try ctx.cli.run(&.{ "worker", "await", "wf-act", "--worker-id", "wd", "--block", "1000" });
    defer none.deinit();
    try stdx.testing.assertStdoutContains(none, "(no tasks)");

    // With the action in "w", the workflow's run is "w"'s to claim.
    try ctx.exec(&.{ "action", "register", "wf-act", "-n", "w" });
    try ctx.exec(&.{ "worker", "register", "ww", "wf-act", "-n", "w" });
    try ctx.exec(&.{ "workflow", "start", "scoped-wf", "{}", "-n", "w" });
    var own = try ctx.cli.run(&.{ "worker", "await", "wf-act", "--worker-id", "ww", "--block", "3000", "-n", "w" });
    defer own.deinit();
    try stdx.testing.assertContains(own, "Task: ");
}

fn readsBack(ctx: *stdx.testing.TestContext, stream: []const u8, ns: []const u8, expected: []const u8) !bool {
    for (0..50) |_| {
        var r = try ctx.cli.run(&.{ "stream", "read", stream, "-n", ns, "--start", "0-0", "--limit", "100" });
        defer r.deinit();
        if (r.stdoutContains(expected)) return true;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return false;
}

test "e2e/scoping: a job with no namespace in its definition runs in the one it was submitted to" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const def =
        \\kind: Processing
        \\name: home-job
        \\sources.[0].stream.name: in
        \\sinks.[0].stream.name: out
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "home-job.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "processing", "submit", path, "-n", "acme" });
    try ctx.exec(&.{ "stream", "append", "in", "acme-record", "-n", "acme" });
    try testing.expect(try readsBack(ctx, "out", "acme", "acme-record"));
    var stray = try ctx.cli.run(&.{ "stream", "read", "out", "--start", "0-0", "--limit", "100" });
    defer stray.deinit();
    try testing.expect(!stray.stdoutContains("acme-record"));
}

test "e2e/scoping: a job reads other namespaces but writes only its own" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const Refused = struct { lines: []const u8, says: []const u8 };
    for ([_]Refused{
        .{ .lines = "sinks.[0].stream.name: out\nsinks.[0].stream.namespace: other", .says = "stream sink 'out' writes namespace 'other'" },
        .{ .lines = "namespace: other\nsinks.[0].stream.name: out", .says = "is not the one it is submitted to" },
    }) |c| {
        var buf: [512]u8 = undefined;
        const def = try std.fmt.bufPrint(&buf, "kind: Processing\nname: refused-job\nsources.[0].stream.name: in\n{s}", .{c.lines});
        const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "refused-job.yaml");
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        var r = try ctx.cli.run(&.{ "processing", "submit", path, "-n", "acme" });
        defer r.deinit();
        try stdx.testing.assertContains(r, c.says);
    }
    // Reading another namespace's stream is allowed.
    const def =
        \\kind: Processing
        \\name: reader-job
        \\sources.[0].stream.name: feed
        \\sources.[0].stream.namespace: shared
        \\sinks.[0].stream.name: copied
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "reader-job.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "processing", "submit", path, "-n", "acme" });
    try ctx.exec(&.{ "stream", "append", "feed", "shared-record", "-n", "shared" });
    try testing.expect(try readsBack(ctx, "copied", "acme", "shared-record"));
}

/// Whether a table printed by the CLI has a row whose first column is `name`.
fn hasRow(table: []const u8, name: []const u8) bool {
    var lines = std.mem.splitScalar(u8, table, '\n');
    while (lines.next()) |line| {
        var cols = std.mem.tokenizeAny(u8, line, " \t");
        if (cols.next()) |first| if (std.mem.eql(u8, first, name)) return true;
    }
    return false;
}

/// Send one request over the client protocol and return its status and
/// message: names holding a NUL cannot be passed as command-line arguments.
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

test "e2e/scoping: a name holding a NUL, which could spell another namespace's key, is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const proto = @import("src").protocol.proto;
    try ctx.exec(&.{ "action", "register", "x", "-n", "b" });

    // In "default" keys are bare, so "b\x00x" would be b's action x.
    var out: [512]u8 = undefined;
    for ([_]proto.OpCode{ .action_register, .action_delete }) |op| {
        const r = try rawCall(ctx, op, "", "b\x00x", "", &out);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "must not contain NUL") != null);
    }
    // b's action x is still there, as it was registered.
    const list = try ctx.execCapture(&.{ "action", "list", "-n", "b" });
    try testing.expect(hasRow(list, "x"));

    // A stream "b" with group "orders\x00g" would be b's group g on "orders".
    var group: [2 + 8]u8 = undefined;
    std.mem.writeInt(u16, group[0..2], 8, .little);
    @memcpy(group[2..], "orders\x00g");
    const g = try rawCall(ctx, proto.OpCode.stream_group_create, "", "b", &group, &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), g.status);
    const w = try rawCall(ctx, proto.OpCode.worker_register, "", "b\x00w", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), w.status);
}

test "e2e/scoping: a worker id in two namespaces is two workers" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "worker", "register", "w", "act", "-n", "a" });
    try ctx.exec(&.{ "worker", "register", "w", "act", "-n", "b" });
    try ctx.exec(&.{ "worker", "drain", "w", "-n", "b" });
    const in_a = try ctx.execCapture(&.{ "worker", "info", "w", "-n", "a" });
    try testing.expect(std.mem.indexOf(u8, in_a, "drain") == null);
    const in_b = try ctx.execCapture(&.{ "worker", "info", "w", "-n", "b" });
    try testing.expect(std.mem.indexOf(u8, in_b, "drain") != null);
    var none = try ctx.cli.run(&.{ "worker", "info", "w", "-n", "c" });
    defer none.deinit();
    try stdx.testing.assertContains(none, "not found");
}

test "e2e/scoping: the dashboard shows an action's runs from its own namespace only" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "act", "-n", "a" });
    try ctx.exec(&.{ "action", "register", "act", "-n", "b" });
    const ra = after(try ctx.execCapture(&.{ "action", "invoke", "act", "{}", "-n", "a" }), "Result: ") orelse return error.NoRunId;
    const rb = after(try ctx.execCapture(&.{ "action", "invoke", "act", "{}", "-n", "b" }), "Result: ") orelse return error.NoRunId;
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for ([_][]const u8{ "/api/v1/actions/act/runs?namespace=a", "/api/v1/actions/act?namespace=a" }) |path| {
        var r = try http.get(path);
        defer r.deinit();
        try testing.expect(r.bodyContains(ra));
        try testing.expect(!r.bodyContains(rb));
    }
}
