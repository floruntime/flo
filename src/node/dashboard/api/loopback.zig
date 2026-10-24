//! The dashboard's loopback client: it reads and writes as any client does,
//! over the node's own protocol port, since the shards' state is theirs
//! alone to touch.

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const DashboardContext = h.DashboardContext;
const client_mod = @import("../../../cli/client/mod.zig");
const reply_pool = @import("../../reply_pool.zig");
const serve = @import("../../../util/http/serve.zig");
const stdx = @import("stdx");

/// The longest one loopback read waits for its answer: the node's own read
/// deadline, and a second to spare.
const LOOPBACK_READ_TIMEOUT_MS: i64 = reply_pool.DEADLINE_MS + std.time.ms_per_s;
/// Kept back from a request's time to write its answer.
const WRITE_ROOM_MS: i64 = 500;

/// A loopback client for one request's reads. The request has one budget,
/// its response's deadline less room to write: every read waits only for
/// what is left of it, so a request of many reads still answers in time,
/// and the dashboard thread is never held past it.
pub const Reader = struct {
    client: client_mod.Client,
    deadline_ms: i64,

    pub fn open(allocator: Allocator, ctx: *DashboardContext) !Reader {
        return openUntil(allocator, ctx, deadlineFor(nowMs(), serve.responseMsLeft()));
    }

    /// The reads' deadline, given what is left of the response's (none
    /// outside a handler): that, less room to write.
    fn deadlineFor(now_ms: i64, response_left_ms: ?i64) i64 {
        return now_ms + (response_left_ms orelse LOOPBACK_READ_TIMEOUT_MS + WRITE_ROOM_MS) - WRITE_ROOM_MS;
    }

    fn openUntil(allocator: Allocator, ctx: *DashboardContext, deadline_ms: i64) !Reader {
        if (deadline_ms <= nowMs()) return error.OutOfTime;
        return .{ .client = try connect(allocator, ctx), .deadline_ms = deadline_ms };
    }

    pub fn deinit(self: *Reader) void {
        self.client.deinit();
    }

    pub fn call(self: *Reader, comptime op: @import("../../../protocol/ops.zig").Op, namespace: []const u8, key: []const u8, body: op.request.Value) !client_mod.Response {
        const left = self.deadline_ms - nowMs();
        if (left <= 0) return error.OutOfTime;
        // Per read(), and an answer may take several: a slow one can run
        // past what is left. On loopback that is a small overrun at most.
        self.client.setReadTimeoutMs(@intCast(@min(left, LOOPBACK_READ_TIMEOUT_MS)));
        return self.client.call(op, namespace, key, body);
    }

    fn nowMs() i64 {
        return @intCast(stdx.time.monotonicMs());
    }
};

/// The answer to a read that didn't come back; the console keeps what it
/// last showed, marked stale.
pub fn readFailed(allocator: Allocator, what: []const u8, err: anyerror) ![]const u8 {
    const msg = switch (err) {
        error.OutOfTime, error.ConnectionTimedOut => try std.fmt.allocPrint(allocator, "{s}: the node didn't answer in time", .{what}),
        else => try std.fmt.allocPrint(allocator, "{s} failed: {s}", .{ what, @errorName(err) }),
    };
    defer allocator.free(msg);
    return h.jsonError(allocator, msg);
}

/// Connect a short-lived loopback client to the node's own protocol port.
/// Mutations can't be proposed from the dashboard thread, so we issue them over
/// the wire to ourselves — the same thread-safe path the CLI uses.
pub fn connect(allocator: Allocator, ctx: *DashboardContext) !client_mod.Client {
    var ep_buf: [32]u8 = undefined;
    const endpoint = try ctx.clientEndpoint(&ep_buf);
    var client = client_mod.Client.init(allocator, endpoint);
    errdefer client.deinit();
    try client.connect();
    return client;
}

test "Reader: a request whose time is spent reads nothing, and says it timed out" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    try std.testing.expectError(error.OutOfTime, Reader.openUntil(allocator, &ctx, Reader.nowMs()));
    // Opened in time, a read once the deadline has passed is refused
    // before any timeout is set.
    var late: Reader = .{ .client = client_mod.Client.init(allocator, "127.0.0.1:1"), .deadline_ms = Reader.nowMs() - 1 };
    defer late.deinit();
    try std.testing.expectError(error.OutOfTime, late.call(@import("../../../protocol/kv_ops.zig").op("kv.get"), "default", "k", .{ .wait_ms = 0, .wait_for = .exists, .routing_key = null, .txn = null }));
    const answer = try readFailed(allocator, "KV scan", error.OutOfTime);
    defer allocator.free(answer);
    try std.testing.expectEqualStrings("{\"error\":\"KV scan: the node didn't answer in time\"}", answer);
}

test "Reader: the reads' deadline is the response's, less room to write" {
    try std.testing.expectEqual(@as(i64, 1000 + 5000 - WRITE_ROOM_MS), Reader.deadlineFor(1000, 5000));
    // Spent: at or before now, so opening refuses.
    try std.testing.expect(Reader.deadlineFor(1000, 0) <= 1000);
    // Outside a handler, one loopback read's worth.
    try std.testing.expectEqual(@as(i64, 1000 + LOOPBACK_READ_TIMEOUT_MS), Reader.deadlineFor(1000, null));
}
