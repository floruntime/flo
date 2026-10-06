//! Whatever is reached by id — a queue message by seq, an action run, a job,
//! a transaction — is reached only from its own namespace, and an action
//! named in two namespaces is two actions. A pipeline reads where it likes
//! and writes only into its own namespace. Each test acts from a second
//! namespace and checks both the answer and that the first is untouched.

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
    try ctx.exec(&.{ "queue", "enqueue", "q", "secret", "-n", "victim" });
    var seq_buf: [24]u8 = undefined;
    const seq = try firstSeq(ctx, "q", "victim", &seq_buf);

    for ([_][]const u8{ "ack", "nack" }) |verb| {
        var r = try ctx.cli.run(&.{ "queue", verb, "q", seq, "-n", "attacker" });
        defer r.deinit();
        try stdx.testing.assertContains(r, "message not found in this queue");
        var other_queue = try ctx.cli.run(&.{ "queue", verb, "other", seq, "-n", "victim" });
        defer other_queue.deinit();
        try stdx.testing.assertContains(other_queue, "message not found in this queue");
    }
    // The message is still there for its own queue, which can ack it.
    var ack = try ctx.cli.run(&.{ "queue", "ack", "q", seq, "-n", "victim" });
    defer ack.deinit();
    try testing.expect(!ack.contains("Error"));
    const left = try ctx.execCapture(&.{ "queue", "dequeue", "q", "-n", "victim", "--timeout", "100" });
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

    var stolen = try ctx.cli.run(&.{ "worker", "await", "act", "--worker-id", "wb", "--block", "500", "-n", "b" });
    defer stolen.deinit();
    try testing.expect(!stolen.contains(run_id));

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
        try testing.expect(!r.contains("stopped") and !r.contains("cancelled") and !r.contains("RUNNING"));
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
    var own = try ctx.cli.run(&.{ "kv", "rollback", tid, "--routing-key", "k", "-n", "a" });
    defer own.deinit();
    try testing.expect(!own.contains("not found"));
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
    var stolen = try ctx.cli.run(&.{ "worker", "await", "wf-act", "--worker-id", "wd", "--block", "1000" });
    defer stolen.deinit();
    try testing.expect(!stolen.contains("Task: "));

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
