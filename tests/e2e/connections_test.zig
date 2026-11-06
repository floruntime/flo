//! Connections that last no time at all: the server closes its side and goes
//! on serving.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const c = std.c;
const posix = std.posix;
const ServerProcess = stdx.testing.ServerProcess;
const proto = @import("src").protocol.proto;
const RequestBuilder = @import("src").protocol.request_builder.RequestBuilder;

const CONNECTIONS = 512;
const BURST = 64;

/// Open connections to `port` and close each at once, some after sending
/// `bytes`: part of a request, or a whole one.
fn shortLivedConnections(port: u16, bytes: []const []const u8) !void {
    const addr: c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    for (0..CONNECTIONS) |i| {
        const fd = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
        try testing.expect(fd >= 0);
        try testing.expectEqual(@as(c_int, 0), c.connect(fd, @ptrCast(&addr), @sizeOf(c.sockaddr.in)));
        // Every other one sends nothing.
        if (i % 2 == 1) {
            const these = bytes[(i / 2) % bytes.len];
            _ = c.write(fd, these.ptr, these.len);
        }
        const no_linger = extern struct { onoff: c_int, linger: c_int }{ .onoff = 1, .linger = 0 };
        _ = c.setsockopt(fd, posix.SOL.SOCKET, posix.SO.LINGER, &no_linger, @sizeOf(@TypeOf(no_linger)));
        _ = c.close(fd);
        if (i % BURST == BURST - 1) stdx.time.sleep(2 * std.time.ns_per_ms);
    }
}

fn expectKvServes(allocator: std.mem.Allocator, server: *ServerProcess) !void {
    var ep: [32]u8 = undefined;
    const cli = try stdx.testing.CliRunner.init(allocator, server.flo_binary, try std.fmt.bufPrint(&ep, "127.0.0.1:{d}", .{server.getPort()}));
    defer cli.deinit();
    var put = try cli.run(&.{ "kv", "set", "after-connections", "still-serving" });
    defer put.deinit();
    var got = try cli.run(&.{ "kv", "get", "after-connections" });
    defer got.deinit();
    try testing.expect(got.stdoutContains("still-serving"));
}

/// An HTTP listener still answers a whole request.
fn expectHttpAnswers(port: u16, path: []const u8) !void {
    const addr: c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    const fd = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    defer _ = c.close(fd);
    try testing.expectEqual(@as(c_int, 0), c.connect(fd, @ptrCast(&addr), @sizeOf(c.sockaddr.in)));
    var req: [128]u8 = undefined;
    const text = try std.fmt.bufPrint(&req, "GET {s} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n", .{path});
    _ = c.write(fd, text.ptr, text.len);
    var answer: [16]u8 = undefined;
    const n = c.read(fd, &answer, answer.len);
    try testing.expect(n > 0);
    try testing.expect(std.mem.startsWith(u8, answer[0..@intCast(n)], "HTTP/1.1 "));
}

test "e2e/connections: short-lived connections leave the client, metrics and dashboard ports serving" {
    const allocator = testing.allocator;
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 1, .metrics_enabled = true, .dashboard_enabled = true });
    defer server.deinit();
    try server.start();

    var builder = RequestBuilder.init(allocator);
    const request = builder.build(proto.OpCode.kv_get, "default", "a-key", "", "");
    var frame: [256]u8 = undefined;
    const whole = try request.serialize(&frame);
    try shortLivedConnections(server.getPort(), &.{ whole[0 .. whole.len / 2], whole });
    try expectKvServes(allocator, server);

    try shortLivedConnections(server.getMetricsPort(), &.{ "GET /me", "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n" });
    try expectHttpAnswers(server.getMetricsPort(), "/metrics");

    try shortLivedConnections(server.getDashboardPort(), &.{ "GET /he", "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n" });
    try expectHttpAnswers(server.getDashboardPort(), "/");
    try expectKvServes(allocator, server);
}

test "e2e/connections: short-lived connections leave the node serving" {
    const allocator = testing.allocator;
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 1, .cluster_enabled = true });
    defer server.deinit();
    try server.start();

    try shortLivedConnections(server.getRaftPort(), &.{"\x00\x01"});
    try expectKvServes(allocator, server);
}
