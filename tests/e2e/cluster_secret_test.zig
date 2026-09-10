//! The peer port is only open to nodes that hold the cluster secret: a
//! joiner with a different one never becomes a peer, and both sides say why.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;

test "e2e/cluster: a joiner with the wrong secret never links, and both sides log the refusal" {
    const allocator = testing.allocator;
    const seed = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1, .cluster_secret = "the-right-one" });
    defer seed.deinit();
    try seed.start();
    const seed_raft = try seed.getRaftEndpoint(allocator);
    defer allocator.free(seed_raft);

    const stranger = try ServerProcess.initWithConfig(allocator, .{ .join_addresses = seed_raft, .shards = 1, .cluster_secret = "another" });
    defer stranger.deinit();
    try stranger.start();
    errdefer {
        seed.dumpLogTail();
        stranger.dumpLogTail();
    }

    // The stranger dials, rejects the seed's proof, backs off and dials
    // again; the seed counts each hang-up after its proof as a wrong
    // secret and warns once a second.
    var attempt: usize = 0;
    while (attempt < 40) : (attempt += 1) {
        if (try stranger.logsContain("wrong cluster secret")) break;
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    try testing.expect(try stranger.logsContain("handshake with"));
    try testing.expect(try stranger.logsContain("wrong cluster secret"));
    try testing.expect(try seed.logsContain("wrong cluster secret"));
    try testing.expect(!try seed.logsContain("connected at"));

    // Nothing written on the seed reaches the stranger.
    const ep_seed = try seed.getEndpoint(allocator);
    defer allocator.free(ep_seed);
    const ep_stranger = try stranger.getEndpoint(allocator);
    defer allocator.free(ep_stranger);
    const cli_seed = try CliRunner.init(allocator, seed.flo_binary, ep_seed);
    defer cli_seed.deinit();
    const cli_stranger = try CliRunner.init(allocator, stranger.flo_binary, ep_stranger);
    defer cli_stranger.deinit();
    var set = try cli_seed.run(&.{ "kv", "set", "only-here", "secret-held" });
    defer set.deinit();
    try stdx.testing.assertSucceeded(set);
    stdx.time.sleep(1500 * std.time.ns_per_ms);
    var get = try cli_stranger.run(&.{ "kv", "get", "only-here" });
    defer get.deinit();
    try testing.expect(!get.contains("secret-held"));
}

test "e2e/cluster: the peer listener refuses to start without a secret" {
    const allocator = testing.allocator;
    const bare = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1, .cluster_secret = "" });
    defer bare.deinit();
    try testing.expectError(error.ServerNotReady, bare.startWithTimeout(3000));
    try testing.expect(try bare.logsContain("[cluster] secret"));
}

test "e2e/cluster: a raft port already in use is a clean refusal, not a crash" {
    const allocator = testing.allocator;
    const first = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1 });
    defer first.deinit();
    try first.start();

    const second = try ServerProcess.initWithConfig(allocator, .{ .cluster_enabled = true, .shards = 1, .raft_port = first.raft_port });
    defer second.deinit();
    try testing.expectError(error.ServerNotReady, second.startWithTimeout(5000));
    try testing.expect(try second.logsContain("BindFailed"));
    try testing.expect(!try second.logsContain("CRASH SIGNAL"));
}
