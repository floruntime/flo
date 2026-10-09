//! Workflow and pipeline definitions whose counts or times don't fit their
//! fields are refused at create or submit, and a timeout past the clock never
//! fires. The node answers each and keeps serving.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const writeDottedToTempYaml = stdx.testing.writeDottedToTempYaml;
const cleanupTempFile = stdx.testing.cleanupTempFile;

fn alive(ctx: *stdx.testing.TestContext) !void {
    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "alive" }), "yes") != null);
}

/// Runs `args` with the definition's file appended; the caller deinits.
fn submit(ctx: *stdx.testing.TestContext, args: []const []const u8, def: []const u8, name: []const u8) !stdx.testing.CommandResult {
    const path = try writeDottedToTempYaml(testing.allocator, def, name);
    defer cleanupTempFile(testing.allocator, path);
    var argv: [8][]const u8 = undefined;
    for (args, 0..) |a, i| argv[i] = a;
    argv[args.len] = path;
    return ctx.cli.run(argv[0 .. args.len + 1]);
}

fn taskId(output: []const u8) ?[]const u8 {
    const start = (std.mem.indexOf(u8, output, "Task: ") orelse return null) + "Task: ".len;
    var end = start;
    while (end < output.len and output[end] != '\n' and output[end] != ' ') end += 1;
    return if (end > start) output[start..end] else null;
}

/// Sends a workflow definition straight to the server and returns the
/// status: the CLI parses a definition itself before sending it, so a bad
/// one never reaches the server through it.
fn createRaw(ctx: *stdx.testing.TestContext, name: []const u8, json: []const u8) !u8 {
    const proto = @import("src").protocol.proto;
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.workflow_create);
    header.request_id = 7;
    const req: proto.Request = .{ .header = header, .namespace = "default", .key = name, .value = json, .options = "" };
    var buf: [1024]u8 = undefined;
    const bytes = try req.serialize(&buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    if (std.c.write(fd, bytes.ptr, bytes.len) != @as(isize, @intCast(bytes.len))) return error.ShortWrite;
    var out: [1024]u8 = undefined;
    var got: usize = 0;
    for (0..300) |_| {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break;
        if (proto.Response.parse(out[0..got])) |resp| return resp.header.status else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NoResponse;
}

fn retryWorkflow(comptime name: []const u8, comptime max_attempts: []const u8) []const u8 {
    return "{\"kind\":\"Workflow\",\"name\":\"" ++ name ++ "\",\"version\":\"1\",\"start\":{\"run\":\"@actions/act\",\"retry\":{\"max_attempts\":" ++ max_attempts ++
        "},\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}";
}

test "e2e/definitions: a workflow whose retry count doesn't fit is refused at create" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const proto = @import("src").protocol.proto;
    try ctx.exec(&.{ "action", "register", "act" });

    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), try createRaw(ctx, "good-retry", retryWorkflow("good-retry", "3")));
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), try createRaw(ctx, "bad-retry", retryWorkflow("bad-retry", "-1")));
    try alive(ctx);
}

test "e2e/definitions: a wait whose timeout is past the clock parks the run, and the node keeps serving" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "far-init" });
    try ctx.exec(&.{ "worker", "register", "far-worker", "far-init" });
    var created = try submit(ctx, &.{ "workflow", "create", "-f" },
        \\kind: Workflow
        \\name: far-wait
        \\version: 1.0.0
        \\start.run: @actions/far-init
        \\start.transitions.success: hold
        \\start.transitions.failure: flo.Failed
        \\steps.hold.wait_for_signal.type: go
        \\steps.hold.wait_for_signal.timeout_ms: 9223372036854775807
        \\steps.hold.transitions.success: flo.Completed
        \\steps.hold.transitions.timeout: flo.Failed
    , "far-wait.yaml");
    defer created.deinit();
    try testing.expect(created.succeeded());
    try ctx.exec(&.{ "workflow", "start", "far-wait", "{}", "--run-id", "far-run" });

    // Complete the first step so the run reaches the wait.
    var task = try ctx.cli.run(&.{ "worker", "await", "far-init", "--worker-id", "far-worker", "--block", "5000" });
    defer task.deinit();
    const id = taskId(task.stdout) orelse return error.NoTask;
    var done = try ctx.cli.run(&.{ "worker", "complete", id, "--worker-id", "far-worker", "--result", "{}" });
    defer done.deinit();

    stdx.time.sleep(500 * std.time.ns_per_ms);
    var status = try ctx.cli.run(&.{ "workflow", "status", "far-run" });
    defer status.deinit();
    try testing.expect(status.contains("waiting"));
    try alive(ctx);
}

test "e2e/definitions: a pipeline whose window or partition doesn't fit is refused at submit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    for ([_][]const u8{
        \\kind: Processing
        \\name: bad-window
        \\sources.[0].stream.name: in
        \\sinks.[0].stream.name: out
        \\operators.[0].type: aggregate
        \\operators.[0].name: agg
        \\operators.[0].function: sum
        \\operators.[0].window: tumbling
        \\operators.[0].window_size: 9223372036854775
        ,
        \\kind: Processing
        \\name: bad-partition
        \\sources.[0].stream.name: in
        \\sources.[0].stream.partitions: -1
        \\sinks.[0].stream.name: out
    }) |def| {
        var r = try submit(ctx, &.{ "processing", "submit" }, def, "bad.yaml");
        r.deinit();
        try alive(ctx);
    }
    var listed = try ctx.cli.run(&.{ "processing", "list" });
    defer listed.deinit();
    try testing.expect(!listed.contains("bad-window") and !listed.contains("bad-partition"));
}
