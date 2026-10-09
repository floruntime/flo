//! A member whose disk is wiped rejoins with --join: it catches up, is
//! guarded until the members have confirmed the term, then votes, and the
//! group elects with it once the old leader is gone. A smoke test of the
//! real path; it would pass without the guard too. The guard's safety is
//! shown in the simulator (src/vopr/simulator.zig), where each hazard has a
//! scenario that fails without its check.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;
const CliRunner = stdx.testing.CliRunner;

test "e2e/cluster: a member whose disk was wiped rejoins guarded, then votes in the next election" {
    const allocator = testing.allocator;
    var cluster = try ClusterContext.initDefault(allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();

    for ([_][]const u8{ "w1", "w2", "w3" }) |k| try cluster.execOnWithRetry(0, &.{ "kv", "set", k, "before-wipe" }, 10, 300);
    allocator.free(try cluster.pollUntilContains(2, &.{ "kv", "get", "w3" }, "before-wipe", 40, 250));

    // Node 2 stops and loses everything but the harness's own files. Its id
    // comes back from inspect, as an operator would read it.
    cluster.stopNode(2);
    const server = cluster.getServer(2).?;
    const offline = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer offline.deinit();
    var inspect = try offline.runRaw(&.{ "server", "inspect", "--config", server.config_file, "--data-dir", server.data_dir });
    defer inspect.deinit();
    const at = std.mem.indexOf(u8, inspect.stdout, "hard state:  node ") orelse return error.NoNodeId;
    const rest = inspect.stdout[at + "hard state:  node ".len ..];
    const node_id = try std.fmt.parseInt(u32, rest[0 .. std.mem.indexOfScalar(u8, rest, ',') orelse return error.NoNodeId], 10);
    for ([_][]const u8{ "00000", "SYSTEM", "flo.pid" }) |name| {
        const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ server.data_dir, name });
        defer allocator.free(path);
        stdx.fs.deleteTree(path) catch {};
    }

    const seed = try cluster.getServer(0).?.getRaftEndpoint(allocator);
    defer allocator.free(seed);
    server.config.node_id = node_id;
    server.config.join_addresses = seed;
    try cluster.restartNode(2);

    // It catches up, and its guard finishes once the members confirm the term.
    allocator.free(try cluster.pollUntilContains(2, &.{ "kv", "get", "w3" }, "before-wipe", 40, 250));
    var attempt: usize = 0;
    while (attempt < 60) : (attempt += 1) {
        if (try server.logsContain("lost-log guard done")) break;
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    try testing.expect(try server.logsContain("joining with nothing in the log and no hard state"));
    try testing.expect(try server.logsContain("lost-log guard done"));

    // The old leader goes; nodes 1 and 2 elect between them, so node 2 votes.
    cluster.stopNode(0);
    try cluster.execOnWithRetry(1, &.{ "kv", "set", "after", "elected-with-wiped" }, 40, 500);
    allocator.free(try cluster.pollUntilContains(2, &.{ "kv", "get", "after" }, "elected-with-wiped", 40, 250));
    allocator.free(try cluster.pollUntilContains(1, &.{ "kv", "get", "w1" }, "before-wipe", 10, 250));
}
