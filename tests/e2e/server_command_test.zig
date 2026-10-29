//! `flo server` commands do what their flags say: a port of 0 is a free one
//! the server reports, stop and status find a server started from a config
//! file, and flags that contradict each other or aren't read are refused.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;

/// Runs `args` with `--endpoint` set to `endpoint`: the runner adds its own
/// after the command's flags, so it can't be given among them.
fn runAt(binary: []const u8, endpoint: []const u8, args: []const []const u8) !stdx.testing.CommandResult {
    const at = try CliRunner.init(testing.allocator, binary, endpoint);
    defer at.deinit();
    return at.run(args);
}

/// Stops whatever server `args` names, even if the test failed before its
/// own stop: a server left running would hold its port.
fn stopAnyway(cli: *CliRunner, args: []const []const u8) void {
    var r = cli.run(args) catch return;
    r.deinit();
}

/// The port `server status` reports for the server in `args`' data dir,
/// polled until the server is up.
fn statusPort(cli: *CliRunner, args: []const []const u8) !u16 {
    for (0..200) |_| {
        var st = try cli.run(args);
        defer st.deinit();
        if (std.mem.indexOf(u8, st.stdout, "Port:")) |at| {
            const rest = std.mem.trimStart(u8, st.stdout[at + "Port:".len ..], " ");
            const end = std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len;
            return try std.fmt.parseInt(u16, rest[0..end], 10);
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    return error.NeverReady;
}

test "e2e/server: --port 0 binds a free port, reports it when ready and in status, and serves on it" {
    const allocator = testing.allocator;
    const server = try ServerProcess.init(allocator);
    defer server.deinit();
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();

    const start = try cli.runAsync(&.{ "server", "start", "--data-dir", server.data_dir, "--port", "0", "--no-metrics", "--no-dashboard" });
    defer start.deinit();
    defer stopAnyway(cli, &.{ "server", "stop", "--data-dir", server.data_dir, "--force" });
    const port = try statusPort(cli, &.{ "server", "status", "--data-dir", server.data_dir });
    // Not the default: a regression that ignores --port 0 binds 9000.
    try testing.expect(port != 0 and port != 9000);

    var ep_buf: [32]u8 = undefined;
    const ep = try std.fmt.bufPrint(&ep_buf, "127.0.0.1:{d}", .{port});
    var set = try runAt(server.flo_binary, ep, &.{ "kv", "set", "k", "v" });
    defer set.deinit();
    try testing.expectEqual(@as(u8, 0), set.exit_code);

    var stop = try cli.run(&.{ "server", "stop", "--data-dir", server.data_dir });
    defer stop.deinit();
    try testing.expectEqual(@as(u8, 0), stop.exit_code);
    var out = try start.waitWithTimeout(10_000);
    defer out.deinit();
    var want_buf: [96]u8 = undefined;
    try testing.expect(out.stdoutContains(try std.fmt.bufPrint(&want_buf, "Flo server ports: client {d}, metrics none, dashboard none\n", .{port})));
    try testing.expect(out.stdoutContains(try std.fmt.bufPrint(&want_buf, "Flo server ready on 0.0.0.0:{d} (peer none, metrics none, dashboard none)\n", .{port})));
}

test "e2e/server: stop and status find a server started from a config file" {
    const allocator = testing.allocator;
    const server = try ServerProcess.init(allocator);
    defer server.deinit();
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();

    const config_path = try std.fmt.allocPrint(allocator, "{s}.toml", .{server.data_dir});
    defer allocator.free(config_path);
    const toml = try std.fmt.allocPrint(allocator, "[server]\nport = 0\ndata_dir = \"{s}\"\n\n[metrics]\nenabled = false\n\n[dashboard]\nenabled = false\n", .{server.data_dir});
    defer allocator.free(toml);
    {
        const f = try stdx.fs.createFile(config_path, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, toml);
    }
    defer stdx.fs.deleteFile(config_path) catch {};

    const start = try cli.runAsync(&.{ "server", "start", "--config", config_path });
    defer start.deinit();
    defer stopAnyway(cli, &.{ "server", "stop", "--config", config_path, "--force" });
    _ = try statusPort(cli, &.{ "server", "status", "--config", config_path });

    var stop = try cli.run(&.{ "server", "stop", "--config", config_path });
    defer stop.deinit();
    try testing.expectEqual(@as(u8, 0), stop.exit_code);
    try testing.expect(stop.stdoutContains("Server stopped."));
    var out = try start.waitWithTimeout(10_000);
    defer out.deinit();

    // Stopped means the data dir is free: a new server starts on it at once.
    const again = try cli.runAsync(&.{ "server", "start", "--config", config_path });
    defer again.deinit();
    defer stopAnyway(cli, &.{ "server", "stop", "--config", config_path, "--force" });
    _ = try statusPort(cli, &.{ "server", "status", "--config", config_path });
    var stop_again = try cli.run(&.{ "server", "stop", "--config", config_path });
    defer stop_again.deinit();
    try testing.expectEqual(@as(u8, 0), stop_again.exit_code);
    var out_again = try again.waitWithTimeout(10_000);
    defer out_again.deinit();
    try testing.expect(!out_again.stderrContains("DataDirInUse"));
}

test "e2e/server: flags that are never read or contradict each other are refused with exit 2" {
    const allocator = testing.allocator;
    const server = try ServerProcess.init(allocator);
    defer server.deinit();
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();

    const cases = [_]struct { endpoint: []const u8 = "127.0.0.1:1", args: []const []const u8, want: []const u8 }{
        .{ .args = &.{ "server", "start", "--data-dir", server.data_dir, "--no-metrics", "--metrics-port", "9999" }, .want = "Error: --metrics-port names a port for the metrics server that --no-metrics turns off; give one or the other\n" },
        .{ .args = &.{ "server", "start", "--data-dir", server.data_dir, "--no-dashboard", "--dashboard-port", "9998" }, .want = "Error: --dashboard-port names a port for the dashboard that --no-dashboard turns off; give one or the other\n" },
        .{ .args = &.{ "server", "metrics", "--format", "prometheus" }, .want = "Error: --format is text or json, not 'prometheus'\n" },
        .{ .endpoint = "localhost:abc", .args = &.{ "server", "metrics" }, .want = "Error: --endpoint localhost:abc: 'abc' is not a port\n" },
        .{ .args = &.{ "server", "start", "--data-dir", server.data_dir, "--threads", "4" }, .want = "Error: unknown flag --threads\n" },
    };
    for (cases) |c| {
        // Bounded: a refusal that regressed would start a server and wait.
        const at = try CliRunner.init(allocator, server.flo_binary, c.endpoint);
        defer at.deinit();
        const cmd = try at.runAsync(c.args);
        defer cmd.deinit();
        var out = try cmd.waitWithTimeout(10_000);
        defer out.deinit();
        try testing.expectEqual(@as(u8, 2), out.exit_code);
        try testing.expectEqualStrings(c.want, out.stderr);
    }
}

test "e2e/server: metrics as json are valid JSON carrying the exposition whole" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .metrics_enabled = true } });
    defer ctx.deinit();
    // A labelled series: the exposition then holds quotes and newlines.
    try ctx.exec(&.{ "stream", "append", "orders", "a" });

    var ep_buf: [32]u8 = undefined;
    const ep = try std.fmt.bufPrint(&ep_buf, "127.0.0.1:{d}", .{ctx.getMetricsPort()});
    var out = try runAt(ctx.server.flo_binary, ep, &.{ "server", "metrics", "--format", "json" });
    defer out.deinit();
    try testing.expectEqual(@as(u8, 0), out.exit_code);
    const parsed = try std.json.parseFromSlice(struct { metrics: []const u8 }, testing.allocator, out.stdout, .{});
    defer parsed.deinit();
    try testing.expect(std.mem.indexOfScalar(u8, parsed.value.metrics, '"') != null);
    try testing.expect(std.mem.indexOfScalar(u8, parsed.value.metrics, '\n') != null);
    try testing.expect(std.mem.indexOf(u8, parsed.value.metrics, "flo_stream_append_records_total{") != null);

    var text = try runAt(ctx.server.flo_binary, ep, &.{ "server", "metrics" });
    defer text.deinit();
    try testing.expectEqual(@as(u8, 0), text.exit_code);
    // The same exposition as text (counters move between the two fetches).
    try testing.expect(text.stdoutContains("flo_stream_append_records_total{"));
    try testing.expect(std.mem.startsWith(u8, text.stdout, "# HELP "));
}

/// Start a server with `--<which>-port` naming a port something else holds,
/// and check it refuses with exit 3, naming the port.
fn expectBindRefused(comptime which: []const u8) !void {
    const allocator = testing.allocator;
    const server = try ServerProcess.init(allocator);
    defer server.deinit();
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();

    // Something else holds the port.
    const held = try stdx.net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer stdx.net.sysClose(held);
    const any = stdx.net.SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, 0);
    try stdx.net.sysBind(held, any.anyPtr(), any.anyLen());
    try stdx.net.sysListen(held, 1);
    const port = (try stdx.net.sysLocalIp4(held)).port;
    var port_buf: [8]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    // Only the listener under test is on; bounded, since a regression
    // starts without it and keeps running.
    const other = if (comptime std.mem.eql(u8, which, "metrics")) "--no-dashboard" else "--no-metrics";
    const start = try cli.runAsync(&.{ "server", "start", "--data-dir", server.data_dir, "--port", "0", other, "--" ++ which ++ "-port", port_arg });
    defer start.deinit();
    defer stopAnyway(cli, &.{ "server", "stop", "--data-dir", server.data_dir, "--force" });
    var out = try start.waitWithTimeout(10_000);
    defer out.deinit();
    try testing.expectEqual(@as(u8, 3), out.exit_code);
    var want_buf: [64]u8 = undefined;
    const want = try std.fmt.bufPrint(&want_buf, "[" ++ which ++ "] cannot listen on 127.0.0.1:{d}", .{port});
    if (!out.stdoutContains(want) and !out.stderrContains(want)) {
        std.debug.print("expected: {s}\nstdout: {s}\nstderr: {s}\n", .{ want, out.stdout, out.stderr });
        return error.TestUnexpectedResult;
    }
}

test "e2e/server: a metrics port already in use refuses the start, naming the port" {
    try expectBindRefused("metrics");
}

test "e2e/server: a dashboard port already in use refuses the start, naming the port" {
    try expectBindRefused("dashboard");
}
