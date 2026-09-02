//! Peers dial each other at the address they advertised.
//!
//! Every node here binds a different loopback address, so a join or a mesh
//! link that fell back to 127.0.0.1 finds nothing listening.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;

fn pollUntilContains(cli: *CliRunner, args: []const []const u8, expected: []const u8) !void {
    var attempt: usize = 0;
    while (attempt < 20) : (attempt += 1) {
        var out = try cli.run(args);
        defer out.deinit();
        if (out.contains(expected)) return;
        if (attempt + 1 == 20) std.debug.print("[peering] last read: stdout={s} stderr={s}\n", .{ out.stdout, out.stderr });
        stdx.time.sleep(500 * std.time.ns_per_ms);
    }
    return error.NeverObserved;
}

test "e2e/cluster: peers are reached at the address they advertise" {
    // All of 127.0.0.0/8 is loopback on Linux; macOS needs an alias per address.
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const allocator = testing.allocator;

    // One shard: only shard 0 has a peer network, so a key hashed elsewhere
    // would never be replicated and the test would measure nothing.
    const seed = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .bind = "127.0.0.2", .shards = 1, .log_level = "debug" });
    defer seed.deinit();
    try seed.start();
    const seed_raft = try seed.getRaftEndpoint(allocator);
    defer allocator.free(seed_raft);

    const n2 = try ServerProcess.initWithConfig(allocator, .{ .bind = "127.0.0.3", .join_addresses = seed_raft, .shards = 1, .log_level = "debug" });
    defer n2.deinit();
    try n2.start();
    const n3 = try ServerProcess.initWithConfig(allocator, .{ .bind = "127.0.0.4", .join_addresses = seed_raft, .shards = 1, .log_level = "debug" });
    defer n3.deinit();
    try n3.start();

    const ep_seed = try seed.getEndpoint(allocator);
    defer allocator.free(ep_seed);
    const ep2 = try n2.getEndpoint(allocator);
    defer allocator.free(ep2);
    const ep3 = try n3.getEndpoint(allocator);
    defer allocator.free(ep3);
    errdefer {
        seed.dumpLogTail();
        n2.dumpLogTail();
        n3.dumpLogTail();
    }
    const cli_seed = try CliRunner.init(allocator, seed.flo_binary, ep_seed);
    defer cli_seed.deinit();
    const cli2 = try CliRunner.init(allocator, n2.flo_binary, ep2);
    defer cli2.deinit();
    const cli3 = try CliRunner.init(allocator, n3.flo_binary, ep3);
    defer cli3.deinit();

    // The join: node 2 could only have found the seed at 127.0.0.2.
    var set1 = try cli_seed.run(&.{ "kv", "set", "from-seed", "v1" });
    defer set1.deinit();
    try stdx.testing.assertSucceeded(set1);
    try pollUntilContains(cli2, &.{ "kv", "get", "from-seed" }, "v1");

    // The mesh: node 3 learned node 2's address from the seed's peer info.
    // With the seed gone, the only path from 2 to 3 is that link.
    seed.stop();
    // Several writes, and the test waits for the last: every node commits
    // into its own index space, so node 2's first entries carry the same
    // indices as the seed's entries node 3 already applied and are dropped
    // on arrival (issue #62). The seed wrote one key here (plus the namespace record the first
    // write persists), so eight leaves margin.
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "from-n2-{d}", .{i});
        var set = try cli2.run(&.{ "kv", "set", key, "v2" });
        defer set.deinit();
        try stdx.testing.assertSucceeded(set);
    }
    try pollUntilContains(cli3, &.{ "kv", "get", "from-n2-7" }, "v2");
}
