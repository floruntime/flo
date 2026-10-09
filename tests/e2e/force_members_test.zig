//! A stopped node's cluster group, read and repaired offline: `flo server
//! inspect` shows it, `flo server force-members` makes the node the only
//! voter and retires its secret, which the server then refuses.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;

test "e2e/cluster: force-members makes a stopped node the only voter and retires its secret, which the server then refuses" {
    const allocator = testing.allocator;
    const node = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1, .cluster_secret = "flo-secret-" ++ "4" ** 64 });
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

    // The old secret is refused, by name, at the next start (and every
    // later one: the record is on disk).
    node.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, node.startWithTimeout(3000));
    try testing.expect(try node.logsContain("retired by force-members"));

    // A new secret: the node leads alone, and the data is still there.
    node.config.cluster_secret = "flo-secret-" ++ "5" ** 64;
    node.dump_log_on_failure = true;
    try node.start();
    const ep = try node.getEndpoint(allocator);
    defer allocator.free(ep);
    const cli = try CliRunner.init(allocator, node.flo_binary, ep);
    defer cli.deinit();
    var get = try cli.run(&.{ "kv", "get", "kept" });
    defer get.deinit();
    try testing.expect(get.stdoutContains("through-force"));
    var set = try cli.run(&.{ "kv", "set", "after", "force" });
    defer set.deinit();
    var after = try cli.run(&.{ "kv", "get", "after" });
    defer after.deinit();
    try testing.expect(after.stdoutContains("force"));
}
