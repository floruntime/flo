//! The CLI refuses an answer whose checksum doesn't match it, exiting with
//! the transport code instead of printing what the damaged bytes say.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

/// Answers one request with an ok answer carrying `body`, the byte at
/// `corrupt_at` of the frame flipped after its checksum was computed.
const DamagingServer = struct {
    listen_fd: std.posix.socket_t,
    port: u16,
    body: []const u8,
    corrupt_at: usize,
    thread: std.Thread = undefined,

    fn start(self: *DamagingServer) !void {
        const net = stdx.net;
        const fd = try net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer net.sysClose(fd);
        const addr = net.SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, 0);
        try net.sysBind(fd, addr.anyPtr(), addr.anyLen());
        try net.sysListen(fd, 1);
        self.listen_fd = fd;
        self.port = (try net.sysLocalIp4(fd)).port;
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
    }

    fn serve(self: *DamagingServer) void {
        const net = stdx.net;
        const conn = net.sysAccept(self.listen_fd, null, null, 0);
        net.sysClose(self.listen_fd);
        const c = conn catch return;
        defer net.sysClose(c);
        var req: [64 * 1024]u8 = undefined;
        var got: usize = 0;
        while (got < @sizeOf(proto.RequestHeader)) {
            const n = net.sysRead(c, req[got..@sizeOf(proto.RequestHeader)]) catch return;
            if (n == 0) return;
            got += n;
        }
        const header = std.mem.bytesToValue(proto.RequestHeader, req[0..@sizeOf(proto.RequestHeader)]);
        var left: usize = header.payload_length;
        while (left > 0) {
            const n = net.sysRead(c, req[0..@min(left, req.len)]) catch return;
            if (n == 0) return;
            left -= n;
        }
        var out: [256]u8 = undefined;
        const frame = proto.Response.serializeOk(header.request_id, self.body, &out) catch return;
        out[self.corrupt_at] ^= 0x01;
        _ = net.sysWrite(c, frame) catch return;
    }

    fn stop(self: *DamagingServer) void {
        // Wake an accept that no command reached, so the join returns.
        if (stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, self.port, 1000)) |fd| stdx.net.sysClose(fd) else |_| {}
        self.thread.join();
    }
};

test "e2e/checksum: an answer that fails its checksum exits 5 and says so" {
    const allocator = testing.allocator;
    const server = try stdx.testing.ServerProcess.init(allocator);
    defer server.deinit();

    // A body byte, then a request_id byte: neither is one the header's
    // magic, version or table checks read.
    for ([_]usize{ @sizeOf(proto.ResponseHeader) + 1, @offsetOf(proto.ResponseHeader, "request_id") }) |offset| {
        var damaging = DamagingServer{ .listen_fd = undefined, .port = 0, .body = &[_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 }, .corrupt_at = offset };
        try damaging.start();
        defer damaging.stop();

        var ep: [32]u8 = undefined;
        const cli = try stdx.testing.CliRunner.init(allocator, server.flo_binary, try std.fmt.bufPrint(&ep, "127.0.0.1:{d}", .{damaging.port}));
        defer cli.deinit();
        const cmd = try cli.runAsync(&.{ "queue", "enqueue", "q", "x" });
        defer cmd.deinit();
        var r = try cmd.waitWithTimeout(10_000);
        defer r.deinit();
        try testing.expectEqual(@as(u8, 5), r.exit_code);
        try testing.expectEqualStrings("Error: the answer from the server failed its checksum\n", r.stderr);
        try testing.expectEqualStrings("", r.stdout);
    }
}
