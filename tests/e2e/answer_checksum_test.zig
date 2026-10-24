//! The CLI refuses an answer whose checksum doesn't match it, or whose
//! length is over what an answer can hold, exiting with the transport code
//! instead of printing what the damaged bytes say.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

/// Answers one request with `status` and `body` (an ok answer's data or a
/// refusal's message), the byte at `corrupt_at` of the frame flipped after
/// its checksum was computed.
const DamagingServer = struct {
    listen_fd: std.posix.socket_t = undefined,
    port: u16 = 0,
    status: proto.StatusCode = .ok,
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
        const frame = if (self.status == .ok)
            proto.Response.serializeOk(header.request_id, self.body, &out) catch return
        else
            proto.Response.serializeRefusal(self.status, header.request_id, proto.unclassified(self.status, self.body), &out);
        out[self.corrupt_at] ^= 0x01;
        _ = net.sysWrite(c, frame) catch return;
    }

    fn stop(self: *DamagingServer) void {
        // Wake an accept that no command reached, so the join returns.
        if (stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, self.port, 1000)) |fd| stdx.net.sysClose(fd) else |_| {}
        self.thread.join();
    }
};

test "e2e/checksum: an answer that fails its checksum or claims too much exits 5 and says so" {
    const allocator = testing.allocator;
    const server = try stdx.testing.ServerProcess.init(allocator);
    defer server.deinit();

    const seq = &[_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 };
    const header_len = @sizeOf(proto.ResponseHeader);
    const request_id = @offsetOf(proto.ResponseHeader, "request_id");
    const failed = "Error: the answer from the server failed its checksum\n";
    // Bytes the header's magic, version and table checks don't read, in an
    // ok answer, an empty one and a refusal; then data_len's high byte.
    const cases = [_]struct { status: proto.StatusCode = .ok, body: []const u8 = seq, offset: usize, want: []const u8 = failed }{
        .{ .offset = header_len + 1 },
        .{ .offset = request_id },
        .{ .body = "", .offset = request_id },
        .{ .status = .conflict, .body = "taken", .offset = header_len + proto.Refusal.HEADER_LEN },
        .{ .offset = @offsetOf(proto.ResponseHeader, "data_len") + 3, .want = std.fmt.comptimePrint("Error: the server's answer claims {d} bytes, more than an answer can hold\n", .{8 + (1 << 24)}) },
    };
    for (cases) |c| {
        var damaging = DamagingServer{ .status = c.status, .body = c.body, .corrupt_at = c.offset };
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
        try testing.expectEqualStrings(c.want, r.stderr);
        try testing.expectEqualStrings("", r.stdout);
    }
}
