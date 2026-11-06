//! A read on a new leader: what the old leader acknowledged is there, or the
//! read is refused for a retry. It is never answered as missing.
//!
//! The window is milliseconds long, so the reads go over sockets held open from
//! before the kill: a CLI process per read arrives after it has closed.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const proto = src.protocol.proto;
const kv_ops = src.protocol.kv_ops;
const ClusterContext = stdx.testing.ClusterContext;

/// Where `cluster.status` puts its term and state (`cluster_wire.Status`).
const TERM_AT = 8;
const ROLE_AT = 16;
const ROLE_LEADER = 2;

const Conn = struct {
    fd: std.c.fd_t,
    buf: [8192]u8 = undefined,
    got: usize = 0,
    next_id: u64 = 1,

    fn open(port: u16) !Conn {
        return .{ .fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 2000) };
    }

    fn close(self: *Conn) void {
        _ = std.c.close(self.fd);
    }

    const Answer = struct { status: u8, reason: proto.Reason = .unclassified, role: u8 = 0, term: u64 = 0, found: bool = false };

    /// One request, and its answer: only what the test reads, as the next call
    /// reuses the buffer. A late answer to a call that gave up is dropped.
    fn call(self: *Conn, op: proto.OpCode, key: []const u8, value: []const u8) !Answer {
        const id = self.next_id;
        self.next_id += 1;
        const req: proto.Request = .{ .header = proto.RequestHeader.init(op, id), .namespace = "", .key = key, .value = value, .options = "" };
        var frame_buf: [256]u8 = undefined;
        const frame = try req.serialize(&frame_buf);
        if (std.c.write(self.fd, frame.ptr, frame.len) != @as(isize, @intCast(frame.len))) return error.ShortWrite;
        const deadline = stdx.time.milliTimestamp() + 5000;
        while (true) {
            if (proto.Response.parse(self.buf[0..self.got])) |r| {
                const total = @sizeOf(proto.ResponseHeader) + r.data.len;
                if (r.header.request_id != id) {
                    std.mem.copyForwards(u8, self.buf[0..], self.buf[total..self.got]);
                    self.got -= total;
                    continue;
                }
                defer {
                    std.mem.copyForwards(u8, self.buf[0..], self.buf[total..self.got]);
                    self.got -= total;
                }
                var a: Answer = .{ .status = r.header.status };
                if (r.header.status != @backingInt(proto.StatusCode.ok)) {
                    a.reason = (try r.refused()).reason;
                } else if (op == .cluster_status) {
                    a.term = std.mem.readInt(u64, r.data[TERM_AT..][0..8], .little);
                    a.role = r.data[ROLE_AT];
                } else {
                    a.found = true;
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
            stdx.time.sleep(100 * std.time.ns_per_us);
        }
    }

    fn status(self: *Conn) !Answer {
        return self.call(.cluster_status, "", "");
    }

    fn get(self: *Conn, key: []const u8) !Answer {
        var body: [64]u8 = undefined;
        return self.call(.kv_get, key, try kv_ops.plainGetBody(&body));
    }
};

/// One kill of the leader: the write it acknowledged is read, as fast as the
/// survivors answer, until a node that leads throughout the read answers it.
fn round(cluster: *ClusterContext, n: usize) !void {
    var conns: [3]?Conn = .{ null, null, null };
    defer for (&conns) |*c| if (c.*) |*open| open.close();
    var tries: usize = 0;
    const old: usize = found: while (true) : (tries += 1) {
        if (tries == 400) return error.NoLeader;
        for (&conns, 0..) |*c, i| {
            if (c.* == null) c.* = Conn.open(cluster.getServer(i).?.getPort()) catch null;
            const conn = if (c.*) |*open| open else continue;
            const s = conn.status() catch continue;
            if (s.status == @backingInt(proto.StatusCode.ok) and s.role == ROLE_LEADER) break :found i;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    };
    for (&conns, 0..) |*c, i| {
        if (c.* == null) c.* = try Conn.open(cluster.getServer(i).?.getPort());
    }

    var key_buf: [16]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "acked-{d}", .{n});
    var body: [64]u8 = undefined;
    const put = try conns[old].?.call(.kv_put, key, try kv_ops.plainSetBody(&body, "there"));
    try testing.expectEqual(@as(u8, @backingInt(proto.StatusCode.ok)), put.status);

    // Acknowledged, and at once the leader is gone.
    cluster.killNode(old);
    conns[old].?.close();
    conns[old] = null;

    const deadline = stdx.time.milliTimestamp() + 30_000;
    while (stdx.time.milliTimestamp() < deadline) {
        for (&conns, 0..) |*c, i| {
            const conn = if (c.*) |*open| open else continue;
            // A follower may lag, so only a read between two answers of
            // leading, in one term, says what a leader answered.
            const before = conn.status() catch continue;
            const got = conn.get(key) catch continue;
            const after = conn.status() catch continue;
            const led = before.role == ROLE_LEADER and after.role == ROLE_LEADER and before.term == after.term;
            if (!led) continue;
            if (got.found) return cluster.restartNode(old);
            if (got.reason != .leader_unknown) {
                std.debug.print("round {d}: node {d} leads term {d} and answered status {d}, {t}\n", .{ n, i, before.term, got.status, got.reason });
                return error.ReadNotRefused;
            }
        }
    }
    return error.NeverAnswered;
}

test "e2e/kv/cluster: a write the old leader acknowledged is never missing on the new leader" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync, .log_level = "info" });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    for (0..8) |n| try round(&cluster, n);
}
