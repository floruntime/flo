//! Membership roles on real servers: joiners enter as replicas, the leader
//! makes up to three voters, an operator promotes past that, and removing a
//! member — the leader included — keeps every acknowledged write.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;

const Member = struct { id: u32, role: []const u8 };
const Members = struct {
    leader: u32,
    members: []const Member,
    removed: []const struct { id: u32, removed_at_ms: u64 },
};

/// `flo cluster members -o json` from `node`. Free with `deinit`.
fn members(cluster: *ClusterContext, node: usize) !std.json.Parsed(Members) {
    const out = try cluster.execCaptureOn(node, &.{ "cluster", "members", "-o", "json" });
    defer cluster.allocator.free(out);
    return std.json.parseFromSlice(Members, cluster.allocator, std.mem.trim(u8, out, " \n"), .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
}

fn countRole(m: Members, role: []const u8) usize {
    var n: usize = 0;
    for (m.members) |x| n += @intFromBool(std.mem.eql(u8, x.role, role));
    return n;
}

/// The id of the member `node` is, read from the leader's view by the
/// address the harness gave it.
fn idOf(cluster: *ClusterContext, node: usize) !u32 {
    const out = try cluster.execCaptureOn(node, &.{ "cluster", "status", "-o", "json" });
    defer cluster.allocator.free(out);
    // "node_id":"flo-xxxxxx" carries the low 24 bits of the id.
    const at = std.mem.indexOf(u8, out, "\"node_id\":\"flo-") orelse return error.NoNodeId;
    const low = try std.fmt.parseInt(u32, out[at + 15 .. at + 21], 16);
    var view = try members(cluster, 0);
    defer view.deinit();
    for (view.value.members) |m| if (@as(u24, @truncate(m.id)) == low) return m.id;
    return error.NotAMember;
}

fn roleOf(cluster: *ClusterContext, node: usize) ![]const u8 {
    const out = try cluster.execCaptureOn(node, &.{ "cluster", "status", "-o", "json" });
    defer cluster.allocator.free(out);
    for ([_][]const u8{ "leader", "follower", "replica", "joining", "removed" }) |r| {
        var buf: [32]u8 = undefined;
        if (std.mem.indexOf(u8, out, try std.fmt.bufPrint(&buf, "\"role\":\"{s}\"", .{r})) != null) return r;
    }
    return "other";
}

fn leaderIndex(cluster: *ClusterContext) !usize {
    for (0..cluster.node_count) |i| {
        if (cluster.getServer(i) == null) continue;
        if (std.mem.eql(u8, roleOf(cluster, i) catch continue, "leader")) return i;
    }
    return error.NoLeader;
}

test "e2e/membership: three nodes form with two joiners promoted in one entry, never two voters" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    var view = try members(&cluster, 0);
    defer view.deinit();
    try testing.expectEqual(@as(usize, 3), countRole(view.value, "voter"));
    const seed = cluster.getServer(0).?;
    try testing.expect(try seed.logsContain("now a voter (3 voters)"));
    try testing.expect(!try seed.logsContain("now a voter (2 voters)"));
}

test "e2e/membership: with two nodes the joiner is a replica: writes go to the leader by name, and commit with the replica gone" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 2 });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try testing.expectEqualStrings("replica", try roleOf(&cluster, 1));

    var redirected = try cluster.clis[1].?.run(&.{ "kv", "set", "k", "v" });
    defer redirected.deinit();
    try stdx.testing.assertFailed(redirected);
    try testing.expect(redirected.contains("this node is a replica; send writes to the leader, node "));
    try testing.expect(redirected.contains(cluster.getNodeEndpoint(0).?));

    // A replica counts toward no quorum: with it gone, writes still commit.
    try cluster.execOn(0, &.{ "kv", "set", "before", "1" });
    allocator_free(&cluster, try cluster.pollUntilContains(1, &.{ "kv", "get", "before" }, "1", 40, 250));
    cluster.stopNode(1);
    try cluster.execOn(0, &.{ "kv", "set", "after", "2" });
    const got = try cluster.execCaptureOn(0, &.{ "kv", "get", "after", "--output", "json" });
    defer cluster.allocator.free(got);
    try testing.expect(std.mem.indexOf(u8, got, "\"value\":\"2\"") != null);
}

fn allocator_free(cluster: *ClusterContext, bytes: []const u8) void {
    cluster.allocator.free(bytes);
}

test "e2e/membership: a fourth node stays a replica until promoted; removal is refused by name, then takes the node out for good" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 4 });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    var replica: usize = 0;
    while (!std.mem.eql(u8, try roleOf(&cluster, replica), "replica")) replica += 1;
    const fourth = try idOf(&cluster, replica);
    var id_buf: [16]u8 = undefined;
    const fourth_s = try std.fmt.bufPrint(&id_buf, "{d}", .{fourth});

    // Promoted from any node: a follower carries it to the leader.
    const via: usize = if (replica == 1) 2 else 1;
    try cluster.execOn(via, &.{ "cluster", "promote", fourth_s });
    var view = try members(&cluster, 0);
    try testing.expectEqual(@as(usize, 4), countRole(view.value, "voter"));
    view.deinit();
    try testing.expect(std.mem.eql(u8, try roleOf(&cluster, replica), "follower") or std.mem.eql(u8, try roleOf(&cluster, replica), "leader"));

    // Four voters down to three needs no confirmation; three to two does.
    const leader = try leaderIndex(&cluster);
    const victim: usize = if (leader == replica) (replica + 1) % 4 else replica;
    const victim_id = try idOf(&cluster, victim);
    var victim_buf: [16]u8 = undefined;
    const victim_s = try std.fmt.bufPrint(&victim_buf, "{d}", .{victim_id});
    try cluster.execOn(leader, &.{ "cluster", "remove", victim_s });
    view = try members(&cluster, leader);
    try testing.expectEqual(@as(usize, 3), countRole(view.value, "voter"));
    try testing.expectEqual(@as(usize, 1), view.value.removed.len);
    try testing.expectEqual(victim_id, view.value.removed[0].id);
    view.deinit();

    var other: usize = 0;
    while (other == leader or other == victim) other += 1;
    const other_id = try idOf(&cluster, other);
    var other_buf: [16]u8 = undefined;
    const other_s = try std.fmt.bufPrint(&other_buf, "{d}", .{other_id});
    var refused = try cluster.clis[leader].?.run(&.{ "cluster", "remove", other_s });
    defer refused.deinit();
    try stdx.testing.assertFailed(refused);
    try testing.expect(refused.contains("leaves 2 voters, which tolerate no failure; add --yes to remove it anyway"));

    // The removed node says so, and stays out across a restart.
    allocator_free(&cluster, try cluster.pollUntilContains(victim, &.{ "cluster", "status" }, "removed", 40, 250));
    var gone = try cluster.clis[victim].?.run(&.{ "kv", "get", "anything" });
    defer gone.deinit();
    try testing.expect(gone.contains("this node was removed from the cluster"));
    cluster.stopNode(victim);
    try cluster.restartNode(victim);
    allocator_free(&cluster, try cluster.pollUntilContains(victim, &.{ "cluster", "status" }, "removed", 40, 250));
    view = try members(&cluster, leader);
    defer view.deinit();
    try testing.expectEqual(@as(usize, 3), view.value.members.len);
}

test "e2e/membership: the leader removes itself: one election, and no acknowledged write is lost" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    const leader = try leaderIndex(&cluster);
    const leader_id = try idOf(&cluster, leader);
    var id_buf: [16]u8 = undefined;
    const leader_s = try std.fmt.bufPrint(&id_buf, "{d}", .{leader_id});
    const term_before = try termOf(&cluster, leader);

    var keys: [20][16]u8 = undefined;
    for (0..10) |i| try cluster.execOn(leader, &.{ "kv", "set", try std.fmt.bufPrint(&keys[i], "before-{d}", .{i}), "v" });
    try cluster.execOn(leader, &.{ "cluster", "remove", leader_s, "--yes" });
    var a: usize = 0;
    while (a == leader) a += 1;
    var b: usize = a + 1;
    while (b == leader) b += 1;
    for (10..20) |i| try cluster.execOnWithRetry(a, &.{ "kv", "set", try std.fmt.bufPrint(&keys[i], "after-{d}", .{i}), "v" }, 40, 250);

    const new_leader = try leaderIndex(&cluster);
    try testing.expect(new_leader != leader);
    try testing.expectEqual(term_before + 1, try termOf(&cluster, new_leader));
    for (0..20) |i| {
        for ([_]usize{ a, b }) |node| {
            const key = if (i < 10) try std.fmt.bufPrint(&keys[i], "before-{d}", .{i}) else try std.fmt.bufPrint(&keys[i], "after-{d}", .{i});
            allocator_free(&cluster, try cluster.pollUntilContains(node, &.{ "kv", "get", key }, "v", 40, 250));
        }
    }
}

fn termOf(cluster: *ClusterContext, node: usize) !u64 {
    const out = try cluster.execCaptureOn(node, &.{ "cluster", "status", "-o", "json" });
    defer cluster.allocator.free(out);
    const at = std.mem.indexOf(u8, out, "\"term\":") orelse return error.NoTerm;
    const rest = out[at + 7 ..];
    const end = std.mem.indexOfAny(u8, rest, ",}") orelse return error.NoTerm;
    return std.fmt.parseInt(u64, rest[0..end], 10);
}
