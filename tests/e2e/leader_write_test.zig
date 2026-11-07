//! A write on a new leader that its handler chooses by reading state: what the
//! old leader acknowledged counts, or the write is refused for a retry. It is
//! never chosen as though that hadn't happened.
//!
//! The window is milliseconds long and ends when the new leader has applied. So
//! the writes before the kill go in one burst, and those after it are waiting on
//! sockets held open on both survivors, for the leader to find when it starts.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const proto = src.protocol.proto;
const kv_ops = src.protocol.kv_ops;
const ClusterContext = stdx.testing.ClusterContext;

const BURST = 300;
const VOLLEY = 32;
const ONE_RECORD = "\x01\x00\x00\x00" ++ "\x01\x00\x00\x00" ++ "a" ++ "\x00\x00";

const Conn = struct {
    fd: std.c.fd_t,
    buf: [65536]u8 = undefined,
    got: usize = 0,
    next_id: u64 = 1,
    /// Answers passed over on the way to the one waited for, that said ok.
    passed_ok: usize = 0,

    const Answer = struct { status: proto.StatusCode, data: [32]u8 = undefined, data_len: usize = 0 };

    fn open(port: u16) !Conn {
        return .{ .fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 2000) };
    }

    fn close(self: *Conn) void {
        _ = std.c.close(self.fd);
    }

    fn send(self: *Conn, op: proto.OpCode, key: []const u8, value: []const u8) !u64 {
        const id = self.next_id;
        self.next_id += 1;
        const req: proto.Request = .{ .header = proto.RequestHeader.init(op, id), .namespace = "", .key = key, .value = value, .options = "" };
        var frame_buf: [512]u8 = undefined;
        const frame = try req.serialize(&frame_buf);
        var off: usize = 0;
        while (off < frame.len) {
            const n = std.c.write(self.fd, frame.ptr + off, frame.len - off);
            if (n <= 0) return error.ShortWrite;
            off += @intCast(n);
        }
        return id;
    }

    /// The answer to `id`. Multi-shard answers can pass each other, but these
    /// clusters run one shard.
    fn wait(self: *Conn, id: u64) !Answer {
        const deadline = stdx.time.milliTimestamp() + 8000;
        while (true) {
            if (proto.Response.parse(self.buf[0..self.got])) |r| {
                const total = @sizeOf(proto.ResponseHeader) + r.data.len;
                defer {
                    std.mem.copyForwards(u8, self.buf[0..], self.buf[total..self.got]);
                    self.got -= total;
                }
                const status: proto.StatusCode = @fromBackingInt(r.header.status);
                if (r.header.request_id != id) {
                    if (status == .ok) self.passed_ok += 1;
                    continue;
                }
                var a: Answer = .{ .status = status };
                if (r.data.len <= a.data.len) {
                    a.data_len = r.data.len;
                    @memcpy(a.data[0..r.data.len], r.data);
                }
                return a;
            } else |_| {}
            const rc = std.c.recv(self.fd, self.buf[self.got..].ptr, self.buf.len - self.got, std.c.MSG.DONTWAIT);
            if (rc > 0) {
                self.got += @intCast(rc);
                continue;
            }
            if (rc == 0) return error.Closed;
            if (stdx.time.milliTimestamp() > deadline) return error.NoAnswer;
            stdx.time.sleep(50 * std.time.ns_per_us);
        }
    }

    fn call(self: *Conn, op: proto.OpCode, key: []const u8, value: []const u8) !Answer {
        return self.wait(try self.send(op, key, value));
    }

    /// A write answered once it commits: every write sent before it committed.
    fn barrier(self: *Conn) !void {
        var body: [64]u8 = undefined;
        try testing.expectEqual(proto.StatusCode.ok, (try self.call(.kv_put, "committed", try kv_ops.plainSetBody(&body, "v"))).status);
    }
};

fn leaderOf(cluster: *ClusterContext) !usize {
    for (0..200) |_| {
        for (0..3) |n| {
            var out = cluster.clis[n].?.run(&.{ "cluster", "status", "-o", "json" }) catch continue;
            defer out.deinit();
            if (out.stdoutContains("\"role\":\"leader\"")) return n;
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

/// Ask until something other than a retryable refusal answers, from either
/// survivor: a follower may have no leader yet.
fn settled(conns: *[2]Conn, op: proto.OpCode, key: []const u8) !proto.StatusCode {
    const deadline = stdx.time.milliTimestamp() + 40_000;
    var which: usize = 0;
    while (stdx.time.milliTimestamp() < deadline) : (which +%= 1) {
        const a = conns[which % 2].call(op, key, "") catch {
            stdx.time.sleep(std.time.ns_per_ms);
            continue;
        };
        if (a.status != .unavailable and a.status != .overloaded) return a.status;
        stdx.time.sleep(200 * std.time.ns_per_us);
    }
    return error.NeverSettled;
}

/// `keys`' writes, the newest first, waiting on both survivors at once. None
/// may answer `wrong`, and what each settles on must be `right`.
fn volley(conns: *[2]Conn, op: proto.OpCode, keys: *const [VOLLEY][]const u8, wrong: proto.StatusCode, right: proto.StatusCode) !void {
    var sent: [2][VOLLEY]u64 = undefined;
    for (keys, 0..) |key, v| for (conns, 0..) |*c, ci| {
        sent[ci][v] = try c.send(op, key, "");
    };
    for (conns, 0..) |*c, ci| for (sent[ci], 0..) |id, v| {
        const a = c.wait(id) catch continue;
        errdefer std.debug.print("{s} on survivor {d}: {t}\n", .{ keys[v], ci, a.status });
        try testing.expect(a.status != wrong);
    };
    for (keys) |key| {
        const status = try settled(conns, op, key);
        errdefer std.debug.print("{s} settled on {t}\n", .{ key, status });
        try testing.expectEqual(right, status);
    }
}

test "e2e/leader-write: after failover, a delete without --force never drops a stream the old leader acknowledged" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    const old = try leaderOf(&cluster);

    var conns: [2]Conn = undefined;
    for (&conns, 1..) |*c, i| c.* = try Conn.open(cluster.getServer((old + i) % 3).?.getPort());
    defer for (&conns) |*c| c.close();
    var writer = try Conn.open(cluster.getServer(old).?.getPort());
    defer writer.close();

    var names: [BURST][16]u8 = undefined;
    var keys: [BURST][]const u8 = undefined;
    for (&names, &keys, 0..) |*name, *key, i| {
        key.* = try std.fmt.bufPrint(name, "acked-{d}", .{i});
        _ = try writer.send(.stream_append, key.*, ONE_RECORD);
    }
    try writer.barrier();
    try testing.expectEqual(@as(usize, BURST), writer.passed_ok);
    cluster.killNode(old);

    var newest: [VOLLEY][]const u8 = undefined;
    for (&newest, 0..) |*key, v| key.* = keys[BURST - 1 - v];
    try volley(&conns, .stream_delete, &newest, .ok, .conflict);
}

test "e2e/leader-write: after failover, a cancel never overwrites how a run the old leader committed finished" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    try cluster.execOnWithRetry(0, &.{ "action", "register", "act" }, 40, 250);
    const old = try leaderOf(&cluster);

    var writer = try Conn.open(cluster.getServer(old).?.getPort());
    defer writer.close();
    const def =
        \\{"kind":"Workflow","name":"flow","version":"1.0.0",
        \\"start":{"run":"@actions/act","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
    ;
    try testing.expectEqual(proto.StatusCode.ok, (try writer.call(.workflow_create, "flow", def)).status);

    var ids: [BURST][32]u8 = undefined;
    var keys: [BURST][]const u8 = undefined;
    for (&ids, &keys) |*id, *key| {
        const started = try writer.call(.workflow_start, "flow", "");
        try testing.expectEqual(proto.StatusCode.ok, started.status);
        @memcpy(id[0..started.data_len], started.data[0..started.data_len]);
        key.* = id[0..started.data_len];
    }

    var conns: [2]Conn = undefined;
    for (&conns, 1..) |*c, i| c.* = try Conn.open(cluster.getServer((old + i) % 3).?.getPort());
    defer for (&conns) |*c| c.close();

    // A cancel is answered as it's proposed, so the barrier says they committed.
    writer.passed_ok = 0;
    for (keys) |key| _ = try writer.send(.workflow_cancel, key, "");
    try writer.barrier();
    try testing.expectEqual(@as(usize, BURST), writer.passed_ok);
    cluster.killNode(old);

    var newest: [VOLLEY][]const u8 = undefined;
    for (&newest, 0..) |*key, v| key.* = keys[BURST - 1 - v];
    try volley(&conns, .workflow_cancel, &newest, .ok, .bad_request);
}
