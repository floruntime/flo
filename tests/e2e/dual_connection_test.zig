//! Regression guard: dual TCP connections must not stall
//! stream_group_join.
//!
//! With N shards, a connection's fd lives on its accept shard while
//! cross-shard blocking-read waiters are registered on the data shard, so
//! responses must be delivered back across shards or they are silently
//! dropped. A healthy join completes in <1s; a stall trips the 5s socket
//! read deadline.
//!
//! Control: single connection group_join (baseline).
//! Guard:   conn A idle + conn B group_join on same namespace.

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

test "e2e/stream: single-connection group_join is fast (control)" {
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
    var resp = src.cli_client.stream.groupJoin(&conn, NS, STREAM, GROUP, "solo-consumer") catch |err| {
        std.debug.print("\n[dual-conn control] single-conn group_join failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer resp.deinit();
    const elapsed = stdx.time.milliTimestamp() - t0;

    std.debug.print("\n[dual-conn control] single-conn group_join: {d}ms status={s}\n", .{ elapsed, @tagName(resp.status) });

    try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
    try testing.expect(elapsed < JOIN_BUDGET_MS);
}

test "e2e/stream: dual-conn group_join across many streams (cross-shard fanout)" {
    // Exercises the async forward path: with shards=4 and stream names whose
    // hashes spread across shards, every group_join is likely to route to a
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
        var resp = src.cli_client.stream.groupJoin(&conn_b, NS, s, GROUP, "fanout-consumer") catch |err| {
            std.debug.print("\n[fanout] group_join on '{s}' failed: {s}\n", .{ s, @errorName(err) });
            return err;
        };
        defer resp.deinit();
        const elapsed = stdx.time.milliTimestamp() - t0;
        try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
        try testing.expect(elapsed < JOIN_BUDGET_MS);
    }
}

test "e2e/stream: dual-connection group_join completes <1s" {
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

    // Conn B: worker — connects then issues group_join on the same namespace.
    var conn_b = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
    defer conn_b.deinit();
    try conn_b.connect();
    conn_b.setReadTimeoutSec(JOIN_READ_TIMEOUT_SEC);

    const t0 = stdx.time.milliTimestamp();
    var resp = src.cli_client.stream.groupJoin(&conn_b, NS, STREAM, GROUP, "worker-consumer") catch |err| {
        const t1 = stdx.time.milliTimestamp();
        std.debug.print(
            "\n[dual-conn guard] dual-conn group_join FAILED after {d}ms: {s}\n" ++
                "             conn A was idle + connected; conn B issued group_join.\n" ++
                "             Expected <1s response, got socket error (stall regressed).\n",
            .{ t1 - t0, @errorName(err) },
        );
        return error.GroupJoinStalled;
    };
    defer resp.deinit();
    const elapsed = stdx.time.milliTimestamp() - t0;

    std.debug.print("\n[dual-conn guard] dual-conn group_join: {d}ms status={s}\n", .{ elapsed, @tagName(resp.status) });

    try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
    try testing.expect(elapsed < JOIN_BUDGET_MS);
}
