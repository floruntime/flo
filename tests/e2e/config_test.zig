//! A node refuses to start on a config it can't honour: a section or key it
//! doesn't know, one that was removed or that nothing reads, and a config
//! file named on the command line that isn't there. Each refusal names the
//! line to fix.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;

fn expectRefused(config: ServerProcess.ServerConfig, says: []const u8) !void {
    const node = try ServerProcess.initWithConfig(testing.allocator, config);
    defer node.deinit();
    try testing.expectError(error.ServerNotReady, node.startWithTimeout(3000));
    if (!try node.logsContain(says)) {
        std.debug.print("expected the log to say: {s}\n", .{says});
        return error.TestUnexpectedResult;
    }
}

test "e2e/config: a section or key the server doesn't read is refused at start, and the refusal names it" {
    try expectRefused(.{ .shards = 1, .extra_config = "[auth]\nenabled = true\n" }, "[auth] was removed");
    try expectRefused(.{ .shards = 1, .extra_config = "[websocket]\nping_interval_ms = 1\n" }, "[websocket] was removed");
    try expectRefused(.{ .shards = 1, .extra_config = "[cold_storage]\nprovider = \"file\"\n" }, "[cold_storage] isn't wired up");
    try expectRefused(.{ .shards = 1, .extra_config = "[metircs]\nenabled = true\n" }, "[metircs] is not a section");
    try expectRefused(.{ .shards = 1, .extra_config = "[cluster]\nelection_timeout_min_ms = 150\n" }, "[cluster] election_timeout_min_ms is not a setting");
}

test "e2e/config: a config file named on the command line that is missing is refused at start" {
    try expectRefused(.{ .shards = 1, .config_file_missing = true }, "not found");
}

test "e2e/config: a node restarted with a partition count other than the one its data was written with is refused, automatic included" {
    const node = try ServerProcess.initWithConfig(testing.allocator, .{ .shards = 1 });
    defer node.deinit();
    // Automatic: 4096 for one shard, recorded as that.
    try node.start();
    node.stop();

    node.config.extra_config = "[server]\npartition_count = 8192\n";
    node.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, node.startWithTimeout(3000));
    const says = "the data was written with 4096 partitions, and this start asks for 8192";
    if (!try node.logsContain(says)) {
        std.debug.print("expected the log to say: {s}\n", .{says});
        return error.TestUnexpectedResult;
    }

    // The same count, explicit, is the same topology.
    node.config.extra_config = "[server]\npartition_count = 4096\n";
    try node.start();
}

test "e2e/config: a node written with an explicit partition count is refused when restarted automatic, if the counts differ" {
    const node = try ServerProcess.initWithConfig(testing.allocator, .{ .shards = 1, .extra_config = "[server]\npartition_count = 8192\n" });
    defer node.deinit();
    try node.start();
    node.stop();

    node.config.extra_config = null;
    node.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, node.startWithTimeout(3000));
    const says = "the data was written with 8192 partitions, and this start asks for 4096 (automatic)";
    if (!try node.logsContain(says)) {
        std.debug.print("expected the log to say: {s}\n", .{says});
        return error.TestUnexpectedResult;
    }
}
