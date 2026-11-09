//! A stopped node's cluster group, read and repaired offline: `flo server
//! inspect` shows it, `flo server force-members` makes the node the only
//! voter and retires its secret, which the server then refuses.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const data_format = @import("src").node.data_format;
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;
const ClusterContext = stdx.testing.ClusterContext;

/// The line `flo server inspect` prints about a node's cluster: its id,
/// generation and next id.
fn clusterLine(allocator: std.mem.Allocator, offline: *CliRunner, server: *ServerProcess) ![]u8 {
    var inspect = try offline.runRaw(&.{ "server", "inspect", "--config", server.config_file, "--data-dir", server.data_dir });
    defer inspect.deinit();
    const at = std.mem.indexOf(u8, inspect.stdout, "cluster:") orelse return error.NoClusterLine;
    const end = std.mem.indexOfScalarPos(u8, inspect.stdout, at, '\n') orelse inspect.stdout.len;
    return allocator.dupe(u8, inspect.stdout[at..end]);
}

test "e2e/cluster: force-members makes a stopped node the only voter and retires its secret, which the server then refuses" {
    const allocator = testing.allocator;
    const node = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1, .cluster_secret = "flo-secret-" ++ stdx.repeat("4", 64) });
    defer node.deinit();
    try node.start();
    errdefer node.dumpLogTail();
    {
        const ep = try node.getEndpoint(allocator);
        defer allocator.free(ep);
        const cli = try CliRunner.init(allocator, node.flo_binary, ep);
        defer cli.deinit();
        var set = try cli.run(&.{ "kv", "set", "kept", "through-force" });
        defer set.deinit();
        var got = try cli.run(&.{ "kv", "get", "kept" });
        defer got.deinit();
        try testing.expect(got.stdoutContains("through-force"));
    }
    node.stop();

    const offline = try CliRunner.init(allocator, node.flo_binary, "127.0.0.1:1");
    defer offline.deinit();
    var inspect = try offline.runRaw(&.{ "server", "inspect", "--config", node.config_file, "--data-dir", node.data_dir });
    defer inspect.deinit();
    try testing.expect(inspect.stdoutContains("log:         indices 1.."));
    try testing.expect(inspect.stdoutContains("config at index"));
    const founded = try clusterLine(allocator, offline, node);
    defer allocator.free(founded);
    try testing.expect(std.mem.indexOf(u8, founded, ", generation 1,") != null);

    // A mistyped path is refused, and not created.
    const missing = try std.fmt.allocPrint(allocator, "{s}/no-such-dir", .{node.data_dir});
    defer allocator.free(missing);
    var refused = try offline.runRaw(&.{ "server", "inspect", "--config", node.config_file, "--data-dir", missing });
    defer refused.deinit();
    try testing.expect(refused.contains("does not exist"));
    try testing.expectError(error.FileNotFound, stdx.fs.openDir(missing, .{}));

    // Without --yes nothing changes.
    var dry = try offline.runRaw(&.{ "server", "force-members", "--config", node.config_file, "--data-dir", node.data_dir });
    defer dry.deinit();
    try testing.expect(dry.stdoutContains("Run again with --yes"));
    // Nothing was retired: the node still starts on its secret.
    try node.start();
    node.stop();

    var forced = try offline.runRaw(&.{ "server", "force-members", "--config", node.config_file, "--data-dir", node.data_dir, "--yes" });
    defer forced.deinit();
    try testing.expect(forced.stdoutContains("is now the only voter"));
    // The same cluster, at the next generation.
    const after_force = try clusterLine(allocator, offline, node);
    defer allocator.free(after_force);
    const id_end = std.mem.indexOf(u8, founded, ", generation").?;
    try testing.expectEqualStrings(founded[0..id_end], after_force[0..id_end]);
    try testing.expect(std.mem.indexOf(u8, after_force, ", generation 2,") != null);

    // The old secret is refused, by name, at the next start (and every
    // later one: the record is on disk).
    node.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, node.startWithTimeout(3000));
    try testing.expect(try node.logsContain("retired by force-members"));

    // A new secret: the node leads alone, and the data is still there.
    node.config.cluster_secret = "flo-secret-" ++ stdx.repeat("5", 64);
    node.dump_log_on_failure = true;
    try node.start();
    const ep = try node.getEndpoint(allocator);
    defer allocator.free(ep);
    const cli = try CliRunner.init(allocator, node.flo_binary, ep);
    defer cli.deinit();
    // Under async durability a restarted member serves reads once it has
    // applied what its group commits: here, once it has elected itself.
    var tries: usize = 0;
    while (true) : (tries += 1) {
        var get = try cli.run(&.{ "kv", "get", "kept" });
        defer get.deinit();
        if (get.stdoutContains("through-force")) break;
        try testing.expect(get.contains("restarted and is catching up"));
        if (tries == 40) return error.NeverCaughtUp;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    var set = try cli.run(&.{ "kv", "set", "after", "force" });
    defer set.deinit();
    var after = try cli.run(&.{ "kv", "get", "after" });
    defer after.deinit();
    try testing.expect(after.stdoutContains("force"));
}

test "e2e/cluster: inspect and force-members refuse a data dir of another format, exit 78, and inspect writes no FORMAT" {
    const allocator = testing.allocator;
    const node = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1, .cluster_secret = "flo-secret-" ++ stdx.repeat("5", 64) });
    defer node.deinit();
    try node.start();
    node.stop();

    const offline = try CliRunner.init(allocator, node.flo_binary, "127.0.0.1:1");
    defer offline.deinit();
    const path = try std.fs.path.join(allocator, &.{ node.data_dir, "FORMAT" });
    defer allocator.free(path);

    // No FORMAT: an older Flo's directory, refused, and left without one.
    try stdx.fs.deleteFile(path);
    var older = try offline.runRaw(&.{ "server", "inspect", "--config", node.config_file, "--data-dir", node.data_dir });
    defer older.deinit();
    try testing.expectEqual(@as(u8, 78), older.exit_code);
    const before_gating = try std.fmt.allocPrint(allocator, "Error: data directory {s} was written by an older Flo (before format gating); this server needs format {d}. There is no migration: run flo server wipe --data-dir {s}", .{ node.data_dir, data_format.CURRENT, node.data_dir });
    defer allocator.free(before_gating);
    try testing.expectEqualStrings(before_gating, std.mem.trim(u8, older.stderr, &std.ascii.whitespace));
    try testing.expectError(error.FileNotFound, stdx.fs.openFile(path, .{}));

    // A newer flo wrote it.
    const f = try stdx.fs.createFile(path, .{ .truncate = true });
    try stdx.fs.writeAll(f, std.fmt.comptimePrint("{d}\n", .{data_format.CURRENT + 1}));
    stdx.fs.closeFile(f);
    const want = try std.fmt.allocPrint(allocator, "Error: data directory format {d}, this binary reads format {d} ({s}); start a flo that reads format {d}", .{ data_format.CURRENT + 1, data_format.CURRENT, path, data_format.CURRENT + 1 });
    defer allocator.free(want);
    for ([_][]const []const u8{
        &.{ "server", "inspect", "--config", node.config_file, "--data-dir", node.data_dir },
        &.{ "server", "force-members", "--config", node.config_file, "--data-dir", node.data_dir, "--yes" },
    }) |args| {
        var refused = try offline.runRaw(args);
        defer refused.deinit();
        try testing.expectEqual(@as(u8, 78), refused.exit_code);
        try testing.expectEqualStrings(want, std.mem.trim(u8, refused.stderr, &std.ascii.whitespace));
    }
}

test "e2e/cluster: every member of a cluster holds its one id, through a restart" {
    const allocator = testing.allocator;
    var cluster = try ClusterContext.initDefault(allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    for (0..3) |n| cluster.allocator.free(try cluster.pollUntilContains(n, &.{ "kv", "get", "up" }, "v", 40, 250));
    for (0..3) |n| cluster.stopNode(n);

    const offline = try CliRunner.init(allocator, cluster.getServer(0).?.flo_binary, "127.0.0.1:1");
    defer offline.deinit();
    const first = try clusterLine(allocator, offline, cluster.getServer(0).?);
    defer allocator.free(first);
    const unset = "cluster:     " ++ stdx.repeat("00", 16);
    try testing.expect(!std.mem.startsWith(u8, first, unset));
    const id_end = std.mem.indexOf(u8, first, ", generation").?;
    for (1..3) |n| {
        const line = try clusterLine(allocator, offline, cluster.getServer(n).?);
        defer allocator.free(line);
        try testing.expectEqualStrings(first[0..id_end], line[0..id_end]);
    }

    for (0..3) |n| try cluster.restartNode(n);
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "again", "v" }, 40, 250);
    cluster.stopNode(0);
    const later = try clusterLine(allocator, offline, cluster.getServer(0).?);
    defer allocator.free(later);
    try testing.expectEqualStrings(first[0..id_end], later[0..id_end]);
}
