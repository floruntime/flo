//! A workflow or pipeline definition with a key the parser doesn't read is
//! refused by name and place, with the same text whether it reaches the
//! server raw or through the CLI (which parses a workflow itself first).

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

const Answer = struct {
    status: u8,
    message_buf: [512]u8 = undefined,
    message_len: usize = 0,

    fn message(self: *const Answer) []const u8 {
        return self.message_buf[0..self.message_len];
    }
};

/// Sends a definition straight to the server, past the CLI's own parse.
fn sendRaw(ctx: *stdx.testing.TestContext, op: proto.OpCode, key: []const u8, definition: []const u8) !Answer {
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(op);
    header.request_id = 11;
    const req: proto.Request = .{ .header = header, .namespace = "default", .key = key, .value = definition, .options = "" };
    var buf: [2048]u8 = undefined;
    const bytes = try req.serialize(&buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    if (std.c.write(fd, bytes.ptr, bytes.len) != @as(isize, @intCast(bytes.len))) return error.ShortWrite;
    var out: [1024]u8 = undefined;
    var got: usize = 0;
    for (0..300) |_| {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break;
        if (proto.Response.parse(out[0..got])) |resp| {
            var answer: Answer = .{ .status = resp.header.status };
            answer.message_len = @min(resp.data.len, answer.message_buf.len);
            @memcpy(answer.message_buf[0..answer.message_len], resp.data[0..answer.message_len]);
            return answer;
        } else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NoResponse;
}

/// Runs `args` with the definition written to a file and appended; returns
/// the CLI's stderr with the trailing newline trimmed. The caller frees it.
fn sendCli(ctx: *stdx.testing.TestContext, args: []const []const u8, definition: []const u8, name: []const u8) ![]const u8 {
    const path = try stdx.testing.writeTempYaml(testing.allocator, definition, name);
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    var argv: [8][]const u8 = undefined;
    for (args, 0..) |a, i| argv[i] = a;
    argv[args.len] = path;
    var result = try ctx.cli.run(argv[0 .. args.len + 1]);
    defer result.deinit();
    try testing.expect(result.exit_code != 0);
    return testing.allocator.dupe(u8, std.mem.trimEnd(u8, result.stderr, "\n"));
}

const bad_workflow =
    \\kind: Workflow
    \\name: typo
    \\version: "1"
    \\start:
    \\  run: "@actions/x"
    \\  transition:
    \\    success: flo.Completed
;
const bad_workflow_message = "unknown key \"transition\" at start";

test "definition keys: a workflow with an unknown key is refused by name, raw and through the CLI, with one text" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const raw = try sendRaw(ctx, .workflow_create, "typo", bad_workflow);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), raw.status);
    try testing.expectEqualStrings(bad_workflow_message, raw.message());

    const cli = try sendCli(ctx, &.{ "workflow", "create", "-f" }, bad_workflow, "typo.yaml");
    defer testing.allocator.free(cli);
    try testing.expectEqualStrings("Error: " ++ bad_workflow_message, cli);

    // Nothing was created.
    var list = try ctx.cli.run(&.{ "workflow", "list" });
    defer list.deinit();
    try testing.expect(std.mem.indexOf(u8, list.stdout, "typo") == null);
}

test "definition keys: a nested unknown key and a camelCase spelling are refused by place" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const nested =
        \\{ "kind": "Workflow", "name": "nested", "version": "1",
        \\  "start": { "run": "@actions/x", "transitions": { "success": "wait" } },
        \\  "steps": { "wait": { "wait_for_signal": { "type": "go", "timeoutMs": 5000 },
        \\                       "transitions": { "success": "flo.Completed" } } } }
    ;
    const a = try sendRaw(ctx, .workflow_create, "nested", nested);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), a.status);
    try testing.expectEqualStrings("unknown key \"timeoutMs\" at steps.wait.wait_for_signal", a.message());
}

const bad_pipeline =
    \\kind: Processing
    \\name: typo-job
    \\sources:
    \\  - stream:
    \\      name: in
    \\      batchSize: 10
    \\sinks:
    \\  - stream:
    \\      name: out
;
const bad_pipeline_message = "unknown key \"batchSize\" at sources[0].stream";

test "definition keys: a pipeline with an unknown key is refused by name, raw and through the CLI, with one text" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const raw = try sendRaw(ctx, .processing_submit, "", bad_pipeline);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), raw.status);
    try testing.expectEqualStrings(bad_pipeline_message, raw.message());

    const cli = try sendCli(ctx, &.{ "processing", "submit" }, bad_pipeline, "typo-job.yaml");
    defer testing.allocator.free(cli);
    try testing.expectEqualStrings("Error: " ++ bad_pipeline_message, cli);

    const op =
        \\{ "kind": "Processing", "sources": [ { "stream": { "name": "in" } } ],
        \\  "operators": [ { "type": "fliter", "condition": "key_not_empty" } ],
        \\  "sinks": [ { "stream": { "name": "out" } } ] }
    ;
    const unknown_op = try sendRaw(ctx, .processing_submit, "", op);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), unknown_op.status);
    try testing.expectEqualStrings("unknown operator type \"fliter\" (filter|passthrough|keyby|aggregate|map|flatmap|kv_lookup|classify) at operators[0]", unknown_op.message());
}

const bad_condition =
    \\kind: Processing
    \\name: typo-filter
    \\sources:
    \\  - stream:
    \\      name: in
    \\operators:
    \\  - type: filter
    \\    condition: "valeu_contains:payment"
    \\sinks:
    \\  - stream:
    \\      name: out
;
const bad_condition_message = "bad condition \"valeu_contains:payment\": unknown condition at operators[0]";

test "definition keys: a filter condition the operator can't evaluate is refused, not run as match-all" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const raw = try sendRaw(ctx, .processing_submit, "", bad_condition);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), raw.status);
    try testing.expectEqualStrings(bad_condition_message, raw.message());

    const cli = try sendCli(ctx, &.{ "processing", "submit" }, bad_condition, "typo-filter.yaml");
    defer testing.allocator.free(cli);
    try testing.expectEqualStrings("Error: " ++ bad_condition_message, cli);
}
