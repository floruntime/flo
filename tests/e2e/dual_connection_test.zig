//! Regression guard: dual TCP connections must not stall
//! stream_group_create.
//!
//! With N shards, a connection's fd lives on its accept shard while
//! cross-shard blocking-read waiters are registered on the data shard, so
//! responses must be delivered back across shards or they are silently
//! dropped. A healthy join completes in <1s; a stall trips the 5s socket
//! read deadline.
//!
//! Control: single connection group_create (baseline).
//! Guard:   conn A idle + conn B group_create on same namespace.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");

const NS: []const u8 = "default";
const STREAM: []const u8 = "dualconn-stream";
const GROUP: []const u8 = "dualconn-cg";

// Read deadline for the join under test: well above a healthy join (~250ms)
// and well below the SDK's 10s I/O timeout, so a stall fails the test fast.
const JOIN_READ_TIMEOUT_SEC: u32 = 5;

// A healthy dual-conn join completes in <1s.
const JOIN_BUDGET_MS: i64 = 1000;

fn seedStream(ctx: *stdx.testing.TestContext) !void {
    // Append once so the stream exists before any join attempts.
    try ctx.exec(&.{ "stream", "append", STREAM, "seed" });
}

test "e2e/stream: single-connection group_create is fast (control)" {
    // Multiple shards so the join can cross shards; with shards=1 every
    // request is shard-local and the stall cannot occur.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    try seedStream(ctx);

    var conn = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
    defer conn.deinit();
    try conn.connect();
    conn.setReadTimeoutSec(JOIN_READ_TIMEOUT_SEC);

    const t0 = stdx.time.milliTimestamp();
    var resp = src.cli_client.stream.groupCreate(&conn, .{ .namespace = NS, .stream = STREAM, .group = GROUP }) catch |err| {
        std.debug.print("\n[dual-conn control] single-conn group_create failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer resp.deinit();
    const elapsed = stdx.time.milliTimestamp() - t0;

    std.debug.print("\n[dual-conn control] single-conn group_create: {d}ms status={s}\n", .{ elapsed, @tagName(resp.status) });

    try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
    try testing.expect(elapsed < JOIN_BUDGET_MS);
}

test "e2e/stream: dual-conn group_create across many streams (cross-shard fanout)" {
    // Exercises the async forward path: with shards=4 and stream names whose
    // hashes spread across shards, every group_create is likely to route to a
    // shard other than the connection's owner — i.e. through forwardToShard +
    // inbox. Each must complete in <1s with an idle parent connection.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    const stream_names = [_][]const u8{
        "fanout-a", "fanout-b", "fanout-c", "fanout-d",
        "fanout-e", "fanout-f", "fanout-g", "fanout-h",
    };
    inline for (stream_names) |s| {
        try ctx.exec(&.{ "stream", "append", s, "seed" });
    }

    var conn_a = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
    defer conn_a.deinit();
    try conn_a.connect();

    stdx.time.sleep(50 * std.time.ns_per_ms);

    var conn_b = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
    defer conn_b.deinit();
    try conn_b.connect();
    conn_b.setReadTimeoutSec(JOIN_READ_TIMEOUT_SEC);

    inline for (stream_names) |s| {
        const t0 = stdx.time.milliTimestamp();
        var resp = src.cli_client.stream.groupCreate(&conn_b, .{ .namespace = NS, .stream = s, .group = GROUP }) catch |err| {
            std.debug.print("\n[fanout] group_create on '{s}' failed: {s}\n", .{ s, @errorName(err) });
            return err;
        };
        defer resp.deinit();
        const elapsed = stdx.time.milliTimestamp() - t0;
        try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
        try testing.expect(elapsed < JOIN_BUDGET_MS);
    }
}

test "e2e/stream: dual-connection group_create completes <1s" {
    // shards=4 so the acceptor round-robins conn A and conn B onto different
    // reactor threads, forcing requests to traverse forwardToShard.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    try seedStream(ctx);

    // Conn A: parent — connects and stays idle for the duration of the test.
    var conn_a = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
    defer conn_a.deinit();
    try conn_a.connect();

    // Brief pause so the server has time to register conn A on its acceptor /
    // shard before conn B arrives.
    stdx.time.sleep(50 * std.time.ns_per_ms);

    // Conn B: worker — connects then issues group_create on the same namespace.
    var conn_b = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
    defer conn_b.deinit();
    try conn_b.connect();
    conn_b.setReadTimeoutSec(JOIN_READ_TIMEOUT_SEC);

    const t0 = stdx.time.milliTimestamp();
    var resp = src.cli_client.stream.groupCreate(&conn_b, .{ .namespace = NS, .stream = STREAM, .group = GROUP }) catch |err| {
        const t1 = stdx.time.milliTimestamp();
        std.debug.print(
            "\n[dual-conn guard] dual-conn group_create FAILED after {d}ms: {s}\n" ++
                "             conn A was idle + connected; conn B issued group_create.\n" ++
                "             Expected <1s response, got socket error (stall regressed).\n",
            .{ t1 - t0, @errorName(err) },
        );
        return error.GroupJoinStalled;
    };
    defer resp.deinit();
    const elapsed = stdx.time.milliTimestamp() - t0;

    std.debug.print("\n[dual-conn guard] dual-conn group_create: {d}ms status={s}\n", .{ elapsed, @tagName(resp.status) });

    try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
    try testing.expect(elapsed < JOIN_BUDGET_MS);
}
