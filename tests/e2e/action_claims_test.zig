//! A run's claim, end to end: what a worker holding it hears back when the
//! lease has moved on, when it resends, when its action lives on another
//! shard, and when the leader that gave it the run dies.
//!
//! Every answer is read off the wire and checked by its status and body:
//! the CLI exits 0 on a refusal, so it can't say which one came back.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const proto = src.protocol.proto;
const action_ops = src.protocol.action_ops;
const schema = src.protocol.schema;
const ClusterContext = stdx.testing.ClusterContext;

const Task = action_ops.Task.Value;

/// Reads one whole response frame from `fd`, or null if none arrives in
/// `wait_ms`.
fn readFrame(fd: std.c.fd_t, buf: []u8, wait_ms: u32) !?proto.Response {
    var got: usize = 0;
    var waited: u32 = 0;
    while (waited < wait_ms) : (waited += 10) {
        const rc = std.c.recv(fd, buf[got..].ptr, buf.len - got, std.c.MSG.DONTWAIT);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) return error.Closed;
        if (proto.Response.parse(buf[0..got])) |r| {
            // Exactly one frame: anything past it is a second answer.
            try testing.expectEqual(got, @sizeOf(proto.ResponseHeader) + r.data.len);
            return r;
        } else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return null;
}

/// One request on `fd`, and its answer.
fn call(fd: std.c.fd_t, op: proto.OpCode, key: []const u8, value: []const u8, buf: []u8, wait_ms: u32) !proto.Response {
    const req: proto.Request = .{ .header = proto.RequestHeader.init(op, 1), .namespace = "", .key = key, .value = value, .options = "" };
    var frame_buf: [1024]u8 = undefined;
    const frame = try req.serialize(&frame_buf);
    if (std.c.write(fd, frame.ptr, frame.len) != @as(isize, @intCast(frame.len))) return error.ShortWrite;
    return (try readFrame(fd, buf, wait_ms)) orelse error.NoAnswer;
}

fn expectOk(r: proto.Response) !void {
    if (r.header.status != @backingInt(proto.StatusCode.ok)) {
        const why = r.refused() catch return error.TestUnexpectedResult;
        std.debug.print("\nrefused ({t}): {s}\n", .{ why.reason, why.message });
        return error.TestUnexpectedResult;
    }
}

/// A task held, copied out of the answer it came in, whose buffer the next
/// call reuses.
const Held = struct {
    task: Task,
    run_buf: [64]u8 = undefined,
    action_buf: [64]u8 = undefined,
    input_buf: [64]u8 = undefined,

    fn copy(self: *Held, t: Task) void {
        self.task = t;
        self.task.run = dupe(&self.run_buf, t.run);
        self.task.action = dupe(&self.action_buf, t.action);
        self.task.input = dupe(&self.input_buf, t.input);
        self.task.namespace = "";
        self.task.caller_run = null;
        self.task.caller_workflow = null;
    }

    fn dupe(buf: []u8, bytes: []const u8) []const u8 {
        @memcpy(buf[0..bytes.len], bytes);
        return buf[0..bytes.len];
    }
};

/// `worker` asks for a task of `action`, waiting `block_ms` at most: the
/// task, or null when the answer carried none.
fn awaitTask(fd: std.c.fd_t, worker: []const u8, action: []const u8, block_ms: u64, held: *Held) !bool {
    return awaitTaskRetrying(fd, worker, action, block_ms, held, 0);
}

/// `awaitTask`, asked again while the node answers that it has no leader
/// ready yet, `retries` times at most.
fn awaitTaskRetrying(fd: std.c.fd_t, worker: []const u8, action: []const u8, block_ms: u64, held: *Held, retries: u32) !bool {
    var names: [action_ops.ActionName.max_size]u8 = undefined;
    var body_buf: [512]u8 = undefined;
    const body = try action_ops.op("action.await").request.encodeInto(&body_buf, .{ .actions = try action_ops.actionNames(&names, &.{action}), .block_ms = block_ms, .request_key = null });
    var buf: [4096]u8 = undefined;
    var left = retries;
    const r = while (true) : (left -= 1) {
        const got = try call(fd, .action_await, worker, body, &buf, @intCast(block_ms + 5000));
        if (left == 0 or got.header.status != @backingInt(proto.StatusCode.unavailable)) break got;
        const why = try got.refused();
        if (why.reason != .leader_unknown) break got;
        stdx.time.sleep(200 * std.time.ns_per_ms);
    };
    try expectOk(r);
    var diag: schema.Diagnostic = .{};
    const task = (try action_ops.op("action.await").response.decodeAll(r.data, &diag)).task orelse return false;
    held.copy(task);
    return true;
}

/// `worker` completes the task it holds with `result`: the answer.
fn complete(fd: std.c.fd_t, worker: []const u8, t: Task, result: []const u8, buf: []u8) !proto.Response {
    var body_buf: [512]u8 = undefined;
    const body = try action_ops.op("action.complete").request.encodeInto(&body_buf, .{ .action = t.action, .worker = worker, .deliveries = t.deliveries, .term = t.term, .index = t.index, .outcome = "", .result = result });
    return call(fd, .action_complete, t.run, body, buf, 5000);
}

const Status = struct { status: action_ops.RunStatus, attempt: u32, worker_buf: [64]u8 = undefined, worker_len: usize = 0, output_buf: [64]u8 = undefined, output_len: usize = 0 };

fn runStatus(fd: std.c.fd_t, run: []const u8) !Status {
    var buf: [4096]u8 = undefined;
    const r = try call(fd, .action_status, run, "", &buf, 5000);
    try expectOk(r);
    var diag: schema.Diagnostic = .{};
    const v = try action_ops.op("action.status").response.decodeAll(r.data, &diag);
    var s: Status = .{ .status = v.status, .attempt = v.attempt };
    if (v.worker_id) |w| {
        @memcpy(s.worker_buf[0..w.len], w);
        s.worker_len = w.len;
    }
    if (v.output) |o| {
        @memcpy(s.output_buf[0..o.len], o);
        s.output_len = o.len;
    }
    return s;
}

/// A run invoked through the CLI: its id, from "Result: <id>".
fn invoke(out: []const u8, buf: []u8) ![]const u8 {
    const prefix = "Result: ";
    if (!std.mem.startsWith(u8, out, prefix)) return error.NoRunId;
    const id = std.mem.trimEnd(u8, out[prefix.len..], "\r\n");
    @memcpy(buf[0..id.len], id);
    return buf[0..id.len];
}

fn connect(port: u16) !std.c.fd_t {
    return stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
}

/// The run's status once it is `want`, polled every 100 ms for `max_ms`.
fn statusOnce(fd: std.c.fd_t, run: []const u8, want: action_ops.RunStatus, max_ms: u64) !Status {
    var waited: u64 = 0;
    while (true) : (waited += 100) {
        const s = try runStatus(fd, run);
        if (s.status == want) return s;
        if (waited >= max_ms) {
            std.debug.print("\nrun {s} is {t}, not {t}, after {d} ms\n", .{ run, s.status, want, max_ms });
            return error.TestUnexpectedResult;
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
}

test "e2e/action/claims: a complete from the holder after its lease expired is refused, saying so" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    // The shortest lease, and attempts left after one expiry.
    try ctx.exec(&.{ "action", "register", "lapse", "--timeout", "5000", "--retries", "3" });
    var run_buf: [64]u8 = undefined;
    const run = try invoke(try ctx.execCapture(&.{ "action", "invoke", "lapse", "{}" }), &run_buf);

    const fd = try connect(ctx.getPort());
    defer _ = std.c.close(fd);
    var held: Held = undefined;
    try testing.expect(try awaitTask(fd, "w1", "lapse", 5000, &held));
    try testing.expectEqualStrings(run, held.task.run);
    // Ready again once the duty ends the lease: its one attempt spent.
    const lapsed = try statusOnce(fd, run, .pending, 15_000);
    try testing.expectEqual(@as(u32, 1), lapsed.attempt);

    var buf: [4096]u8 = undefined;
    const r = try complete(fd, "w1", held.task, "late", &buf);
    try testing.expectEqual(@backingInt(proto.StatusCode.conflict), r.header.status);
    const why = try r.refused();
    try testing.expectEqual(proto.Reason.stale_report, why.reason);
    try testing.expectEqual(proto.Ran.no, why.ran);
    try testing.expectEqualStrings("stale report: the lease expired and the run moved on", why.message);
    // Its result not taken.
    const s = try runStatus(fd, run);
    try testing.expectEqual(action_ops.RunStatus.pending, s.status);
    try testing.expectEqual(@as(usize, 0), s.output_len);
}

test "e2e/action/claims: once a run is given to another worker, the first's complete names a delivery since replaced, and is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "again", "--timeout", "5000", "--retries", "3" });
    var run_buf: [64]u8 = undefined;
    const run = try invoke(try ctx.execCapture(&.{ "action", "invoke", "again", "{}" }), &run_buf);

    const fd = try connect(ctx.getPort());
    defer _ = std.c.close(fd);
    var first: Held = undefined;
    try testing.expect(try awaitTask(fd, "w1", "again", 5000, &first));
    _ = try statusOnce(fd, run, .pending, 15_000);
    // Delivered again, to another worker: a new token.
    var second: Held = undefined;
    try testing.expect(try awaitTask(fd, "w2", "again", 5000, &second));
    try testing.expectEqualStrings(run, second.task.run);
    try testing.expectEqual(@as(u32, 2), second.task.deliveries);
    try testing.expect(second.task.index != first.task.index);

    var buf: [4096]u8 = undefined;
    const stale = try complete(fd, "w1", first.task, "late", &buf);
    try testing.expectEqual(@backingInt(proto.StatusCode.conflict), stale.header.status);
    const why = try stale.refused();
    try testing.expectEqual(proto.Reason.stale_report, why.reason);
    try testing.expectEqualStrings("stale report: the run was delivered again since", why.message);
    // The holder, the delivery count right, but another delivery's token:
    // the token alone tells them apart.
    var forged = second.task;
    forged.index = first.task.index;
    const wrong = try complete(fd, "w2", forged, "forged", &buf);
    try testing.expectEqual(@backingInt(proto.StatusCode.conflict), wrong.header.status);
    try testing.expectEqualStrings("stale report: the run was delivered again since", (try wrong.refused()).message);
    // The holder's complete applies; neither other result was taken.
    try expectOk(try complete(fd, "w2", second.task, "on time", &buf));
    const s = try runStatus(fd, run);
    try testing.expectEqual(action_ops.RunStatus.completed, s.status);
    try testing.expectEqual(@as(u32, 2), s.attempt);
    try testing.expectEqualStrings("w2", s.worker_buf[0..s.worker_len]);
    try testing.expectEqualStrings("on time", s.output_buf[0..s.output_len]);
}

test "e2e/action/claims: a complete resent after it applied answers ok again, and the run keeps the first" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "twice" });
    var run_buf: [64]u8 = undefined;
    const run = try invoke(try ctx.execCapture(&.{ "action", "invoke", "twice", "{}" }), &run_buf);

    const fd = try connect(ctx.getPort());
    defer _ = std.c.close(fd);
    var held: Held = undefined;
    try testing.expect(try awaitTask(fd, "w1", "twice", 5000, &held));
    var buf: [4096]u8 = undefined;
    try expectOk(try complete(fd, "w1", held.task, "done", &buf));
    // The worker never heard, say, and sends it again.
    try expectOk(try complete(fd, "w1", held.task, "done", &buf));
    const s = try runStatus(fd, run);
    try testing.expectEqual(action_ops.RunStatus.completed, s.status);
    try testing.expectEqual(@as(u32, 1), s.attempt);
    try testing.expectEqualStrings("done", s.output_buf[0..s.output_len]);
    // Another result for the same delivery is told the run has one.
    const other = try complete(fd, "w1", held.task, "other", &buf);
    try testing.expectEqual(@backingInt(proto.StatusCode.conflict), other.header.status);
    try testing.expectEqualStrings("stale report: the run was completed with another result", (try other.refused()).message);
}

test "e2e/action/claims: on 4 shards, a worker whose shard isn't its action's is given its runs, waiting or not" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();
    // A worker and an action on different shards, as the server routes
    // them: by namespace and key, over as many partitions as it picks for
    // 4 shards when none are configured.
    const router = src.node.router.Router.init(0, 4, 0);
    const shardOf = struct {
        fn f(r: src.node.router.Router, key: []const u8) u16 {
            return switch (r.route(src.node.router.hashKeyWithNamespace("default", key))) {
                .local => 0,
                .shard => |s| s.shard_id,
            };
        }
    }.f;
    const worker = "xs-worker";
    var name_buf: [16]u8 = undefined;
    const action = for (0..64) |i| {
        const name = try std.fmt.bufPrint(&name_buf, "xs-act-{d}", .{i});
        if (shardOf(router, name) != shardOf(router, worker)) break name;
    } else return error.NoAction;
    try testing.expect(shardOf(router, action) != shardOf(router, worker));
    try ctx.exec(&.{ "action", "register", action });

    const fd = try connect(ctx.getPort());
    defer _ = std.c.close(fd);
    var buf: [4096]u8 = undefined;
    var run_buf: [64]u8 = undefined;
    // A run already there.
    const first = try invoke(try ctx.execCapture(&.{ "action", "invoke", action, "one" }), &run_buf);
    var held: Held = undefined;
    try testing.expect(try awaitTask(fd, worker, action, 5000, &held));
    try testing.expectEqualStrings(first, held.task.run);
    try testing.expectEqualStrings("one", held.task.input);
    try expectOk(try complete(fd, worker, held.task, "r1", &buf));
    try testing.expectEqual(action_ops.RunStatus.completed, (try runStatus(fd, first)).status);

    // A run invoked while the worker waits: the action's shard wakes it.
    var names: [action_ops.ActionName.max_size]u8 = undefined;
    var body_buf: [512]u8 = undefined;
    const body = try action_ops.op("action.await").request.encodeInto(&body_buf, .{ .actions = try action_ops.actionNames(&names, &.{action}), .block_ms = 10_000, .request_key = null });
    const req: proto.Request = .{ .header = proto.RequestHeader.init(.action_await, 7), .namespace = "", .key = worker, .value = body, .options = "" };
    var frame_buf: [1024]u8 = undefined;
    const frame = try req.serialize(&frame_buf);
    if (std.c.write(fd, frame.ptr, frame.len) != @as(isize, @intCast(frame.len))) return error.ShortWrite;
    try testing.expectEqual(@as(?proto.Response, null), try readFrame(fd, &buf, 300));
    const asked_at = stdx.time.milliTimestamp();
    var second_buf: [64]u8 = undefined;
    const second = try invoke(try ctx.execCapture(&.{ "action", "invoke", action, "two" }), &second_buf);
    const r = (try readFrame(fd, &buf, 10_000)) orelse return error.NoAnswer;
    try expectOk(r);
    try testing.expectEqual(@as(u64, 7), r.header.request_id);
    var diag: schema.Diagnostic = .{};
    const task = (try action_ops.op("action.await").response.decodeAll(r.data, &diag)).task orelse return error.NoTask;
    try testing.expectEqualStrings(second, task.run);
    // Woken when it came, not at the end of its block.
    try testing.expect(stdx.time.milliTimestamp() - asked_at < 5000);
}

// =============================================================================
// Leader failover
// =============================================================================

fn roleOf(cluster: *ClusterContext, node: usize) ![]const u8 {
    const out = try cluster.execCaptureOn(node, &.{ "cluster", "status", "-o", "json" });
    defer cluster.allocator.free(out);
    for ([_][]const u8{ "leader", "follower", "replica" }) |role| {
        var buf: [32]u8 = undefined;
        if (std.mem.indexOf(u8, out, try std.fmt.bufPrint(&buf, "\"role\":\"{s}\"", .{role})) != null) return role;
    }
    return "other";
}

/// The node that leads, waiting for one: `skip` is down.
fn leaderOf(cluster: *ClusterContext, skip: ?usize) !usize {
    for (0..100) |_| {
        for (0..cluster.node_count) |i| {
            if (skip == i or cluster.getServer(i) == null) continue;
            if (std.mem.eql(u8, roleOf(cluster, i) catch continue, "leader")) return i;
        }
        stdx.time.sleep(200 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

test "e2e/action/cluster: a run claimed before its leader is killed stays its worker's: same attempt, not given again, and its complete applies" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    const first = try leaderOf(&cluster, null);
    // A lease long enough to outlast the failover and the new leader's grace.
    try cluster.execOn(first, &.{ "action", "register", "survive", "--timeout", "60000", "--retries", "3" });
    var run_buf: [64]u8 = undefined;
    const invoked = try cluster.execCaptureOn(first, &.{ "action", "invoke", "survive", "{}" });
    defer cluster.allocator.free(invoked);
    const run = try invoke(invoked, &run_buf);

    var held: Held = undefined;
    {
        const fd = try connect(cluster.getServer(first).?.getPort());
        defer _ = std.c.close(fd);
        // Answered at commit: a majority holds the delivery.
        try testing.expect(try awaitTask(fd, "w1", "survive", 5000, &held));
    }
    try testing.expectEqualStrings(run, held.task.run);
    try testing.expectEqual(@as(u32, 1), held.task.attempt);
    try testing.expectEqual(@as(u32, 1), held.task.deliveries);

    // A crash: no shutdown, nothing flushed on the way out.
    cluster.killNode(first);
    const next = try leaderOf(&cluster, first);
    const fd = try connect(cluster.getServer(next).?.getPort());
    defer _ = std.c.close(fd);
    // Another worker waits through three of the new leader's expire
    // ticks: a takeover that ended or freed the lease would hand it the
    // run, and wake it to say so.
    var other: Held = undefined;
    try testing.expect(!try awaitTaskRetrying(fd, "w2", "survive", 3000, &other, 25));
    const s = try runStatus(fd, run);
    try testing.expectEqual(action_ops.RunStatus.running, s.status);
    try testing.expectEqual(@as(u32, 1), s.attempt);
    try testing.expectEqualStrings("w1", s.worker_buf[0..s.worker_len]);
    // The holder's complete, naming the delivery the dead leader made, applies.
    var buf: [4096]u8 = undefined;
    try expectOk(try complete(fd, "w1", held.task, "ok", &buf));
    const done = try runStatus(fd, run);
    try testing.expectEqual(action_ops.RunStatus.completed, done.status);
    try testing.expectEqual(@as(u32, 1), done.attempt);
    try testing.expectEqualStrings("ok", done.output_buf[0..done.output_len]);
}
