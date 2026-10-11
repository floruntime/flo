//! The node's probes: `/health` (the process answers), `/ready` (every
//! shard can serve) and `/status` (each shard's state). HTTP is up before
//! the shards replay, so a node replaying a large log says so instead of
//! refusing the connection.
//!
//! These assert on status and body, not on a request merely succeeding.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

fn expectAnswer(http: anytype, path: []const u8, status: u16, body: []const u8) !void {
    var resp = try http.get(path);
    defer resp.deinit();
    if (resp.status != status or !std.mem.eql(u8, resp.bodyTrimmed(), body)) {
        std.debug.print("GET {s}: got {d} '{s}', want {d} '{s}'\n", .{ path, resp.status, resp.bodyTrimmed(), status, body });
        return error.TestUnexpectedResult;
    }
}

test "e2e/health: a healthy node is live and ready on both ports, and /status names its shards" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .metrics_enabled = true, .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var dashboard = try ctx.createDashboardHttp();
    defer dashboard.deinit();
    var metrics = try ctx.createMetricsHttp();
    defer metrics.deinit();
    try expectAnswer(dashboard, "/health", 200, "ok");
    try expectAnswer(dashboard, "/ready", 200, "ready");
    try expectAnswer(metrics, "/health", 200, "ok");
    try expectAnswer(metrics, "/ready", 200, "ready");

    var resp = try metrics.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try testing.expect(root.get("ready").?.bool);
    const shards = root.get("shards").?.array.items;
    try testing.expectEqual(@as(usize, 2), shards.len);
    for (shards) |s| {
        try testing.expectEqualStrings("ok", s.object.get("state").?.string);
        try testing.expectEqualStrings("leader", s.object.get("role").?.string);
    }
}

/// Flip a byte in the middle of every sealed segment shard `shard` holds.
fn corruptSegments(data_dir: []const u8, shard: u16) !usize {
    var path_buf: [512]u8 = undefined;
    const segs = try std.fmt.bufPrint(&path_buf, "{s}/{d:0>5}/segs", .{ data_dir, shard });
    var dir = try stdx.fs.openDir(segs, .{ .iterate = true });
    defer stdx.fs.closeDir(dir);
    var corrupted: usize = 0;
    var iter = dir.iterate();
    while (try iter.next(stdx.io.instance())) |de| {
        if (!std.mem.endsWith(u8, de.name, ".flseg")) continue;
        var file_buf: [600]u8 = undefined;
        const path = try std.fmt.bufPrint(&file_buf, "{s}/{s}", .{ segs, de.name });
        const bytes = blk: {
            const f = try stdx.fs.openFile(path, .{});
            defer stdx.fs.closeFile(f);
            break :blk try stdx.fs.readToEndAlloc(f, testing.allocator, 1 << 24);
        };
        defer testing.allocator.free(bytes);
        bytes[bytes.len / 2] ^= 0xff;
        const f = try stdx.fs.createFile(path, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, bytes);
        corrupted += 1;
    }
    return corrupted;
}

test "e2e/health: a lone node with a damaged shard stays ready, and /status says which shard and why" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true, .dashboard_enabled = true },
    });
    defer ctx.deinit();
    var key_buf: [16]u8 = undefined;
    for (0..24) |i| try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "key-{d}", .{i}), "kept" });

    ctx.server.stop();
    try testing.expect(try corruptSegments(ctx.server.getDataDir(), 1) > 0);
    try ctx.restartServer();

    var dashboard = try ctx.createDashboardHttp();
    defer dashboard.deinit();
    var metrics = try ctx.createMetricsHttp();
    defer metrics.deinit();
    try expectAnswer(metrics, "/health", 200, "ok");
    // Nothing else could take this node's traffic: a 503 would pull its
    // healthy shard too.
    try expectAnswer(metrics, "/ready", 200, "ready");
    try expectAnswer(dashboard, "/ready", 200, "ready");

    var resp = try metrics.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
    defer parsed.deinit();
    const shards = parsed.value.object.get("shards").?.array.items;
    try testing.expectEqualStrings("ok", shards[0].object.get("state").?.string);
    const damaged = shards[1].object;
    try testing.expectEqualStrings("offline", damaged.get("state").?.string);
    try testing.expect(std.mem.startsWith(u8, damaged.get("reason").?.string, "segment"));
    try testing.expect(damaged.get("losses").?.object.get("segment_unreadable").?.integer > 0);
}

/// Write `count` keys over one connection, `batch` requests in flight at
/// a time, each answered ok.
fn putMany(port: u16, count: usize, batch: usize) !void {
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
    defer _ = std.c.close(fd);
    const out = try testing.allocator.alloc(u8, batch * 128);
    defer testing.allocator.free(out);
    const in = try testing.allocator.alloc(u8, batch * 64 + 4096);
    defer testing.allocator.free(in);

    var next: usize = 0;
    while (next < count) {
        const n = @min(batch, count - next);
        var used: usize = 0;
        for (0..n) |i| {
            var key_buf: [24]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "replay-{d}", .{next + i});
            const req: proto.Request = .{ .header = proto.RequestHeader.init(.kv_put, next + i + 1), .namespace = "", .key = key, .value = "v", .options = "" };
            used += (try req.serialize(out[used..])).len;
        }
        var sent: usize = 0;
        while (sent < used) {
            const rc = std.c.write(fd, out[sent..].ptr, used - sent);
            if (rc <= 0) return error.ShortWrite;
            sent += @intCast(rc);
        }
        var got: usize = 0;
        var answered: usize = 0;
        var off: usize = 0;
        while (answered < n) {
            if (proto.Response.parse(in[off..got])) |r| {
                if (r.header.status != @intFromEnum(proto.StatusCode.ok)) return error.PutRefused;
                off += @sizeOf(proto.ResponseHeader) + r.header.data_len;
                answered += 1;
                continue;
            } else |_| {}
            if (off > 0) {
                std.mem.copyForwards(u8, in[0 .. got - off], in[off..got]);
                got -= off;
                off = 0;
            }
            const rc = std.c.read(fd, in[got..].ptr, in.len - got);
            if (rc == 0) return error.ConnectionClosed;
            if (rc < 0) {
                // The socket does not block; wait for the answers.
                if (std.c._errno().* != @intFromEnum(std.c.E.AGAIN)) return error.ReadFailed;
                stdx.time.sleep(std.time.ns_per_ms / 4);
                continue;
            }
            got += @intCast(rc);
        }
        next += n;
    }
}

const ReadyWatch = struct {
    server: *stdx.testing.ServerProcess,
    saw_replaying_ms: ?i64 = null,
    saw_ready_ms: ?i64 = null,
    words: [8][]const u8 = undefined,
    word_count: usize = 0,

    /// Poll /ready on the restarted server's metrics port, noting each
    /// change of answer. The old server has stopped, so until the new one
    /// listens nothing answers, whichever port is read.
    fn run(self: *ReadyWatch) void {
        const start = stdx.time.milliTimestamp();
        var last: []const u8 = "";
        while (stdx.time.milliTimestamp() - start < 120_000) {
            // The harness picks the new port before it spawns the server.
            const port = @atomicLoad(u16, &self.server.metrics_port, .acquire);
            const word = probe(port) orelse {
                stdx.time.sleep(std.time.ns_per_ms);
                continue;
            };
            if (!std.mem.eql(u8, word, last) and self.word_count < self.words.len) {
                self.words[self.word_count] = word;
                self.word_count += 1;
                last = word;
            }
            const now = stdx.time.milliTimestamp();
            if (std.mem.eql(u8, word, "503 replaying") and self.saw_replaying_ms == null) self.saw_replaying_ms = now;
            if (std.mem.eql(u8, word, "200 ready")) {
                self.saw_ready_ms = now;
                return;
            }
            stdx.time.sleep(2 * std.time.ns_per_ms);
        }
    }

    /// "<status> <word>" from GET /ready, or null when nothing answers yet.
    fn probe(port: u16) ?[]const u8 {
        const fd = stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 200) catch return null;
        defer _ = std.c.close(fd);
        const req = "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n";
        if (std.c.write(fd, req, req.len) != req.len) return null;
        var buf: [512]u8 = undefined;
        var got: usize = 0;
        // The socket does not block: read until the server closes it.
        const deadline = stdx.time.milliTimestamp() + 1000;
        while (got < buf.len and stdx.time.milliTimestamp() < deadline) {
            const rc = std.c.read(fd, buf[got..].ptr, buf.len - got);
            if (rc == 0) break;
            if (rc < 0) {
                stdx.time.sleep(std.time.ns_per_ms / 4);
                continue;
            }
            got += @intCast(rc);
        }
        const answer = buf[0..got];
        const body = if (std.mem.indexOf(u8, answer, "\r\n\r\n")) |i| std.mem.trim(u8, answer[i + 4 ..], "\r\n ") else return null;
        const status: []const u8 = if (std.mem.startsWith(u8, answer, "HTTP/1.1 200")) "200" else if (std.mem.startsWith(u8, answer, "HTTP/1.1 503")) "503" else "???";
        for (known) |k| {
            if (std.mem.startsWith(u8, k, status) and std.mem.eql(u8, k[4..], body)) return k;
        }
        return "unexpected";
    }

    const known = [_][]const u8{ "200 ready", "503 loading", "503 replaying" };
};

test "e2e/health: a node replaying its log answers /ready with 503 replaying, then 200 ready" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();
    // A real log, not a delay built in: in a Debug build this many entries
    // take a few seconds to replay.
    try putMany(ctx.getPort(), 40_000, 512);

    ctx.server.stop();
    var watch: ReadyWatch = .{ .server = ctx.server };
    const watcher = try std.Thread.spawn(.{}, ReadyWatch.run, .{&watch});
    const restart_ms = stdx.time.milliTimestamp();
    ctx.restartServer() catch |err| {
        watcher.join();
        return err;
    };
    watcher.join();

    const unexpected = for (watch.words[0..watch.word_count]) |w| {
        if (std.mem.eql(u8, w, "unexpected")) break true;
    } else false;
    if (watch.saw_replaying_ms == null or watch.saw_ready_ms == null or unexpected) {
        for (watch.words[0..watch.word_count]) |w| std.debug.print("/ready answered {s}\n", .{w});
        std.debug.print("replaying seen at +{?d} ms, ready at +{?d} ms\n", .{
            if (watch.saw_replaying_ms) |t| t - restart_ms else null,
            if (watch.saw_ready_ms) |t| t - restart_ms else null,
        });
        return error.TestUnexpectedResult;
    }
    try testing.expect(watch.saw_replaying_ms.? <= watch.saw_ready_ms.?);
}
