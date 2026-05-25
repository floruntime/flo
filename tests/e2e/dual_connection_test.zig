//! FLO-093 regression guard: dual TCP connections must not stall
//! stream_group_join.
//!
//! Two concurrent TCP connections to the same namespace previously hung
//! `stream_group_join` on the second connection (>10s, then I/O timeout)
//! because cross-shard blocking-read waiters were registered on the data
//! shard, while the connection's fd lived on the accept shard — so
//! getConnection(fd) returned null on the data shard and responses were
//! silently dropped. With N shards, ~(N-1)/N of blocking reads were
//! affected.
//!
//! Fixed by 22cfa45 (deliver blocking-read responses across shards) and
//! e9c73e8 (guard cross-shard deferred response against fd reuse). This
//! test pins that path: a healthy join completes in <1s; a regression
//! that re-introduces the wedge will trip the 5s socket read deadline.
//!
//! Control: single connection group_join (baseline).
//! Guard:   conn A idle + conn B group_join on same namespace.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");

const NS: []const u8 = "default";
const STREAM: []const u8 = "flo093-stream";
const GROUP: []const u8 = "flo093-cg";

// Read deadline for the join under test: well above a healthy join (~250ms)
// and well below the SDK's 10s I/O timeout, so a stall fails the test fast.
const JOIN_READ_TIMEOUT_SEC: u32 = 5;

// Acceptance criterion from the ticket: dual-conn join completes in <1s.
const JOIN_BUDGET_MS: i64 = 1000;

fn seedStream(ctx: *stdx.testing.TestContext) !void {
    // Append once so the stream exists before any join attempts.
    try ctx.exec(&.{ "stream", "append", STREAM, "seed" });
}

test "e2e/stream: FLO-093 control — single connection group_join is fast" {
    // Use multiple shards so requests can exercise the cross-shard
    // forwardToShard path. The ticket's root cause is that forwardToShard
    // runs synchronously on the connection-owner reactor; with shards=1 the
    // bug is invisible because every request is always shard-local.
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
        std.debug.print("\n[FLO-093 control] single-conn group_join failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer resp.deinit();
    const elapsed = stdx.time.milliTimestamp() - t0;

    std.debug.print("\n[FLO-093 control] single-conn group_join: {d}ms status={s}\n", .{ elapsed, @tagName(resp.status) });

    try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
    try testing.expect(elapsed < JOIN_BUDGET_MS);
}

test "e2e/stream: FLO-093 repro — dual-connection group_join completes <1s" {
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
    // shard before conn B arrives. Mirrors the probe's "parent connected, then
    // worker connects" timing.
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
            "\n[FLO-093 repro] dual-conn group_join FAILED after {d}ms: {s}\n" ++
                "             conn A was idle + connected; conn B issued group_join.\n" ++
                "             Expected <1s response, got socket error (stall reproduces).\n",
            .{ t1 - t0, @errorName(err) },
        );
        return error.GroupJoinStalled;
    };
    defer resp.deinit();
    const elapsed = stdx.time.milliTimestamp() - t0;

    std.debug.print("\n[FLO-093 repro] dual-conn group_join: {d}ms status={s}\n", .{ elapsed, @tagName(resp.status) });

    try testing.expectEqual(@as(@TypeOf(resp.status), .ok), resp.status);
    try testing.expect(elapsed < JOIN_BUDGET_MS);
}
