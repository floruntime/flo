//! Consumer groups on the delivery model: a read leases each record it
//! takes until it is acked, nacked or its lease runs out, and every replica
//! holds the same leases, so a restart or a new leader keeps them.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;

/// The deliveries a `-o json` group read printed, comma-joined, in order.
fn deliveries(buf: []u8, out: []const u8) ![]const u8 {
    var w: usize = 0;
    var it = std.mem.splitSequence(u8, out, "\"delivery\":\"");
    _ = it.next();
    while (it.next()) |rest| {
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse return error.Malformed;
        if (w > 0) {
            buf[w] = ',';
            w += 1;
        }
        @memcpy(buf[w..][0..end], rest[0..end]);
        w += end;
    }
    return buf[0..w];
}

/// The `n`th (from 0) delivery of a comma-joined list.
fn nth(list: []const u8, n: usize) []const u8 {
    var it = std.mem.splitScalar(u8, list, ',');
    for (0..n) |_| _ = it.next();
    return it.next() orelse "";
}

fn read(ctx: *stdx.testing.TestContext, stream: []const u8, consumer: []const u8, limit: []const u8) !stdx.testing.CommandResult {
    const out = try ctx.cli.run(&.{ "stream", "group", "read", stream, "--group", "g", "--consumer", consumer, "--limit", limit, "-o", "json" });
    try testing.expectEqual(@as(u8, 0), out.exit_code);
    return out;
}

fn infoHas(ctx: *stdx.testing.TestContext, stream: []const u8, want: []const u8) !bool {
    var info = try ctx.cli.run(&.{ "stream", "group", "info", stream, "--group", "g", "-o", "json" });
    defer info.deinit();
    if (!info.stdoutContains(want)) std.debug.print("group info: {s}{s}\n", .{ info.stdout, info.stderr });
    return info.stdoutContains(want);
}

test "e2e/group: acking one record of an append acks only that record" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "a", "b", "c" });
    var r = try read(ctx, "s", "w", "3");
    defer r.deinit();
    var db: [512]u8 = undefined;
    const ds = try deliveries(&db, r.stdout);
    var ack = try ctx.cli.run(&.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "w", "--ids", nth(ds, 1) });
    defer ack.deinit();
    try testing.expect(ack.stdoutContains("Acknowledged 1 record(s)"));
    try testing.expect(try infoHas(ctx, "s", "\"ready\":0,\"leased\":2,\"dead\":0"));
    // The same ack again finds it gone.
    var again = try ctx.cli.run(&.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "w", "--ids", nth(ds, 1) });
    defer again.deinit();
    try testing.expect(again.stdoutContains("Acknowledged 0 record(s)"));
    try testing.expect(again.stderrContains("gone"));
}

test "e2e/group: a nacked record is the next read's, delivered again, and --dlq dead-letters it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "one" });
    try ctx.exec(&.{ "stream", "append", "s", "two" });
    var first = try read(ctx, "s", "w", "1");
    defer first.deinit();
    try testing.expect(first.stdoutContains("\"data\":\"one\""));
    var db: [256]u8 = undefined;
    var nack = try ctx.cli.run(&.{ "stream", "group", "nack", "s", "--group", "g", "--consumer", "w", "--ids", try deliveries(&db, first.stdout), "--error", "boom" });
    defer nack.deinit();
    try testing.expect(nack.stdoutContains("Failed 1 record(s)"));
    // Ready again, it comes before the record past the cursor.
    var second = try read(ctx, "s", "w", "1");
    defer second.deinit();
    try testing.expect(second.stdoutContains("\"data\":\"one\""));
    try testing.expect(second.stdoutContains("\"deliveries\":2"));
    var dlq = try ctx.cli.run(&.{ "stream", "group", "nack", "s", "--group", "g", "--consumer", "w", "--ids", try deliveries(&db, second.stdout), "--dlq" });
    defer dlq.deinit();
    try testing.expect(dlq.stdoutContains("Failed 1 record(s)"));
    try testing.expect(try infoHas(ctx, "s", "\"ready\":0,\"leased\":0,\"dead\":1"));
    var third = try read(ctx, "s", "w", "1");
    defer third.deinit();
    try testing.expect(third.stdoutContains("\"data\":\"two\""));
}

test "e2e/group: a lease that runs out gives the record to the next read" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "group", "create", "s", "--group", "g", "--ack-timeout", "5000" });
    try ctx.exec(&.{ "stream", "append", "s", "late" });
    var first = try read(ctx, "s", "slow", "1");
    defer first.deinit();
    try testing.expect(first.stdoutContains("\"data\":\"late\""));
    // A new leader's duty waits out a grace of up to the lease first.
    var tries: usize = 0;
    while (true) : (tries += 1) {
        if (tries == 60) return error.NeverRedelivered;
        stdx.time.sleep(500 * std.time.ns_per_ms);
        var r = try read(ctx, "s", "fast", "1");
        defer r.deinit();
        if (!r.stdoutContains("\"data\":\"late\"")) continue;
        try testing.expect(r.stdoutContains("\"deliveries\":2"));
        break;
    }
    // The first holder's ack is stale.
    var db: [256]u8 = undefined;
    var ack = try ctx.cli.run(&.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "slow", "--ids", try deliveries(&db, first.stdout) });
    defer ack.deinit();
    try testing.expect(ack.stdoutContains("Acknowledged 0 record(s)"));
}

test "e2e/group: a lease below 5 s, or attempts of 0, are refused at create" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    var short = try ctx.cli.run(&.{ "stream", "group", "create", "s", "--group", "g", "--ack-timeout", "1000" });
    defer short.deinit();
    try testing.expectEqual(@as(u8, 2), short.exit_code);
    try testing.expect(short.stderrContains("--ack-timeout is 5000 to 43200000 ms, not 1000"));
    var none = try ctx.cli.run(&.{ "stream", "group", "create", "s", "--group", "g", "--max-deliver", "0" });
    defer none.deinit();
    try testing.expectEqual(@as(u8, 2), none.exit_code);
    try testing.expect(none.stderrContains("--max-deliver is 1 to 255, not 0"));
}

test "e2e/group: leases survive a restart, and an ack after it applies" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .durability = .sync } });
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "a" });
    try ctx.exec(&.{ "stream", "append", "s", "b" });
    var r = try read(ctx, "s", "w", "2");
    defer r.deinit();
    var db: [512]u8 = undefined;
    const ds = try deliveries(&db, r.stdout);
    try ctx.restartServer();
    try testing.expect(try infoHas(ctx, "s", "\"ready\":0,\"leased\":2,\"dead\":0"));
    var ack = try ctx.cli.run(&.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "w", "--ids", ds });
    defer ack.deinit();
    try testing.expect(ack.stdoutContains("Acknowledged 2 record(s)"));
    try testing.expect(try infoHas(ctx, "s", "\"ready\":0,\"leased\":0,\"dead\":0"));
}

test "e2e/group: a trim's records leave the group, never delivered again" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    for ([_][]const u8{ "r0", "r1", "r2" }) |p| try ctx.exec(&.{ "stream", "append", "s", p });
    var r = try read(ctx, "s", "w", "3");
    defer r.deinit();
    var db: [512]u8 = undefined;
    const ds = try deliveries(&db, r.stdout);
    var nack = try ctx.cli.run(&.{ "stream", "group", "nack", "s", "--group", "g", "--consumer", "w", "--ids", ds });
    defer nack.deinit();
    try testing.expect(nack.stdoutContains("Failed 3 record(s)"));
    try ctx.exec(&.{ "stream", "trim", "s", "--maxlen", "1" });
    // Cleanup takes the two trimmed ones out within a duty tick or two.
    var tries: usize = 0;
    while (!try infoHas(ctx, "s", "\"ready\":1,\"leased\":0,\"dead\":0,\"trimmed_pending\":0")) : (tries += 1) {
        if (tries == 40) return error.NeverCleaned;
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    var again = try read(ctx, "s", "w", "3");
    defer again.deinit();
    try testing.expect(again.stdoutContains("\"data\":\"r2\""));
    try testing.expect(!again.stdoutContains("\"data\":\"r0\""));
    try testing.expect(!again.stdoutContains("\"data\":\"r1\""));
}

test "e2e/group: a deleted stream's groups are gone, and a stream of that name starts over" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "old" });
    var r = try read(ctx, "s", "w", "1");
    defer r.deinit();
    try ctx.exec(&.{ "stream", "delete", "s", "--force" });
    var info = try ctx.cli.run(&.{ "stream", "group", "info", "s", "--group", "g" });
    defer info.deinit();
    try testing.expect(info.exit_code != 0);
    try testing.expect(info.stderrContains("consumer group not found"));
    try ctx.exec(&.{ "stream", "append", "s", "new" });
    // A read creates the group again once the old one's records are cleaned.
    var tries: usize = 0;
    while (true) : (tries += 1) {
        if (tries == 40) return error.NeverStartedOver;
        var again = try ctx.cli.run(&.{ "stream", "group", "read", "s", "--group", "g", "--consumer", "w", "-o", "json" });
        defer again.deinit();
        if (again.stdoutContains("\"data\":\"new\"")) {
            try testing.expect(!again.stdoutContains("\"data\":\"old\""));
            try testing.expect(again.stdoutContains("\"deliveries\":1"));
            break;
        }
        try testing.expect(again.exit_code == 0 or again.stderrContains("still being removed"));
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
}

test "e2e/group: a create of a group that exists says so with its settings, and is refused with others, naming its own" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    var made = try ctx.cli.run(&.{ "stream", "group", "create", "s", "--group", "g", "--ack-timeout", "6000", "--max-deliver", "3" });
    defer made.deinit();
    try testing.expect(made.stdoutContains("Created consumer group"));
    var same = try ctx.cli.run(&.{ "stream", "group", "create", "s", "--group", "g", "--ack-timeout", "6000", "--max-deliver", "3" });
    defer same.deinit();
    try testing.expectEqual(@as(u8, 0), same.exit_code);
    try testing.expect(same.stdoutContains("exists, with these settings"));
    var other = try ctx.cli.run(&.{ "stream", "group", "create", "s", "--group", "g", "--ack-timeout", "9000" });
    defer other.deinit();
    try testing.expect(other.exit_code != 0);
    try testing.expect(other.stderrContains("exists with a lease (ack timeout) of 6000 ms and 3 attempts (max deliver)"));
}

test "e2e/group: several records in one ack and in one nack each apply" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "a", "b", "c", "d" });
    var r = try read(ctx, "s", "w", "4");
    defer r.deinit();
    var db: [1024]u8 = undefined;
    const ds = try deliveries(&db, r.stdout);
    var two: [2][256]u8 = undefined;
    const first = try std.fmt.bufPrint(&two[0], "{s},{s}", .{ nth(ds, 0), nth(ds, 1) });
    const last = try std.fmt.bufPrint(&two[1], "{s},{s}", .{ nth(ds, 2), nth(ds, 3) });
    var ack = try ctx.cli.run(&.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "w", "--ids", first });
    defer ack.deinit();
    try testing.expect(ack.stdoutContains("Acknowledged 2 record(s)"));
    var nack = try ctx.cli.run(&.{ "stream", "group", "nack", "s", "--group", "g", "--consumer", "w", "--ids", last });
    defer nack.deinit();
    try testing.expect(nack.stdoutContains("Failed 2 record(s)"));
    try testing.expect(try infoHas(ctx, "s", "\"ready\":2,\"leased\":0,\"dead\":0"));
}

test "e2e/group: a touch keeps a record past its first lease" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "group", "create", "s", "--group", "g", "--ack-timeout", "5000" });
    try ctx.exec(&.{ "stream", "append", "s", "kept" });
    var r = try read(ctx, "s", "w", "1");
    defer r.deinit();
    var db: [256]u8 = undefined;
    const ds = try deliveries(&db, r.stdout);
    // With half the lease or less left, a full lease from now.
    stdx.time.sleep(3000 * std.time.ns_per_ms);
    var touch = try ctx.cli.run(&.{ "stream", "group", "touch", "s", "--group", "g", "--consumer", "w", "--ids", ds });
    defer touch.deinit();
    try testing.expect(touch.stdoutContains("Kept 1 record(s)"));
    // Past the first lease's end: still the holder's.
    stdx.time.sleep(3500 * std.time.ns_per_ms);
    var other = try read(ctx, "s", "x", "1");
    defer other.deinit();
    try testing.expect(!other.stdoutContains("\"data\":\"kept\""));
    var ack = try ctx.cli.run(&.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "w", "--ids", ds });
    defer ack.deinit();
    try testing.expect(ack.stdoutContains("Acknowledged 1 record(s)"));
}

test "e2e/group: a group read carries each record's headers" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "x", "--header", "source=web,v=2" });
    var r = try read(ctx, "s", "w", "1");
    defer r.deinit();
    try testing.expect(r.stdoutContains("\"headers\":{\"source\":\"web\",\"v\":\"2\"}"));
}

test "e2e/group: a group reads every partition of its stream" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "create", "s", "--partitions", "4" });
    for ([_][]const u8{ "0", "1", "2", "3" }, [_][]const u8{ "p0", "p1", "p2", "p3" }) |part, p| try ctx.exec(&.{ "stream", "append", "s", p, "--partition", part });
    var r = try read(ctx, "s", "w", "10");
    defer r.deinit();
    for ([_][]const u8{ "\"data\":\"p0\"", "\"data\":\"p1\"", "\"data\":\"p2\"", "\"data\":\"p3\"" }) |want| try testing.expect(r.stdoutContains(want));
}

test "e2e/group: retention's trims leave the group as a trim's do" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "create", "s", "--retention-count", "2" });
    try ctx.exec(&.{ "stream", "group", "create", "s", "--group", "g" });
    try ctx.exec(&.{ "stream", "append", "s", "r0" });
    var held = try read(ctx, "s", "w", "1");
    defer held.deinit();
    try testing.expect(held.stdoutContains("\"data\":\"r0\""));
    for ([_][]const u8{ "r1", "r2", "r3", "r4" }) |p| try ctx.exec(&.{ "stream", "append", "s", p });
    // Once retention has run and cleanup taken what it cut: the last two.
    var tries: usize = 0;
    while (!try infoHas(ctx, "s", "\"ready\":0,\"leased\":0,\"dead\":0,\"trimmed_pending\":0")) : (tries += 1) {
        if (tries == 80) return error.NeverCleaned;
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    var r = try read(ctx, "s", "w", "10");
    defer r.deinit();
    try testing.expect(r.stdoutContains("\"data\":\"r3\""));
    try testing.expect(r.stdoutContains("\"data\":\"r4\""));
    for ([_][]const u8{ "\"data\":\"r0\"", "\"data\":\"r1\"", "\"data\":\"r2\"" }) |gone| try testing.expect(!r.stdoutContains(gone));
}

test "e2e/group: a deleted group's name can be created again once its records are cleaned" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "a", "b" });
    var r = try read(ctx, "s", "w", "2");
    defer r.deinit();
    try ctx.exec(&.{ "stream", "group", "delete", "s", "--group", "g" });
    // Refused retryable while its records are cleaned, then made anew.
    var tries: usize = 0;
    while (true) : (tries += 1) {
        if (tries == 40) return error.NeverCreated;
        var c = try ctx.cli.run(&.{ "stream", "group", "create", "s", "--group", "g" });
        defer c.deinit();
        if (c.stdoutContains("Created consumer group")) break;
        try testing.expect(c.stderrContains("still being removed"));
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    // From the stream's start, delivered for the first time.
    var again = try read(ctx, "s", "w", "2");
    defer again.deinit();
    try testing.expect(again.stdoutContains("\"data\":\"a\""));
    try testing.expect(!again.stdoutContains("\"deliveries\":2"));
}

test "e2e/group: a large trimmed backlog is passed over: a read parks, and cleanup takes it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    // 300 records in one append, then a tail: all read, the 300 nacked
    // (ready), the tail kept (leased).
    var args: [3 + 300][]const u8 = undefined;
    args[0..3].* = .{ "stream", "append", "s" };
    for (args[3..]) |*a| a.* = "x";
    try ctx.exec(&args);
    try ctx.exec(&.{ "stream", "append", "s", "tail" });
    var reads: [3]stdx.testing.CommandResult = undefined;
    for (&reads) |*r| r.* = try read(ctx, "s", "w", "100");
    defer for (&reads) |*r| r.deinit();
    var tail = try read(ctx, "s", "w", "100");
    defer tail.deinit();
    try testing.expect(tail.stdoutContains("\"data\":\"tail\""));
    for (&reads) |*r| {
        var db: [100 * 64]u8 = undefined;
        var nack = try ctx.cli.run(&.{ "stream", "group", "nack", "s", "--group", "g", "--consumer", "w", "--ids", try deliveries(&db, r.stdout) });
        defer nack.deinit();
        try testing.expect(nack.stdoutContains("Failed 100 record(s)"));
    }
    try ctx.exec(&.{ "stream", "trim", "s", "--maxlen", "1" });
    // None to give: the read waits out its block with nothing.
    const t0 = stdx.time.milliTimestamp();
    var parked = try ctx.cli.run(&.{ "stream", "group", "read", "s", "--group", "g", "--consumer", "v", "--limit", "100", "--block", "1000", "-o", "json" });
    defer parked.deinit();
    try testing.expectEqual(@as(u8, 0), parked.exit_code);
    try testing.expect(parked.stdoutContains("[]"));
    try testing.expect(stdx.time.milliTimestamp() - t0 >= 900);
    var tries: usize = 0;
    while (!try infoHas(ctx, "s", "\"ready\":0,\"leased\":1,\"dead\":0,\"trimmed_pending\":0")) : (tries += 1) {
        if (tries == 80) return error.NeverCleaned;
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
}

fn isLeader(cluster: *ClusterContext, node: usize) bool {
    if (cluster.clis[node] == null) return false;
    var out = cluster.clis[node].?.run(&.{ "cluster", "status", "-o", "json" }) catch return false;
    defer out.deinit();
    return out.stdoutContains("\"role\":\"leader\"");
}

test "e2e/group: a new leader keeps the old one's leases, and an ack of them applies" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "stream", "append", "s", "a" }, 20, 250);
    try cluster.execOnWithRetry(0, &.{ "stream", "append", "s", "b" }, 20, 250);
    const out = try cluster.execCaptureAnyOn(0, &.{ "stream", "group", "read", "s", "--group", "g", "--consumer", "w", "--limit", "2", "-o", "json" });
    defer testing.allocator.free(out);
    var db: [512]u8 = undefined;
    const ds = try deliveries(&db, out);
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, ds, ","));
    const leader: usize = for (0..3) |n| {
        if (isLeader(&cluster, n)) break n;
    } else return error.NoLeader;
    cluster.killNode(leader);
    const other: usize = if (leader == 0) 1 else 0;
    var tries: usize = 0;
    while (true) : (tries += 1) {
        if (tries == 80) return error.AckNeverApplied;
        stdx.time.sleep(250 * std.time.ns_per_ms);
        const ack = cluster.execCaptureAnyOn(other, &.{ "stream", "group", "ack", "s", "--group", "g", "--consumer", "w", "--ids", ds }) catch continue;
        defer testing.allocator.free(ack);
        if (std.mem.indexOf(u8, ack, "Acknowledged 2 record(s)") != null) break;
    }
}

test "e2e/group: a removed group op is refused, naming what replaces it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const client_mod = @import("src").cli_client;
    var conn = client_mod.Client.init(testing.allocator, ctx.endpoint);
    defer conn.deinit();
    try conn.connect();
    const cases = [_]struct { op: @import("src").protocol.proto.OpCode, says: []const u8 }{
        .{ .op = .stream_group_join, .says = "groups have no join or leave" },
        .{ .op = .stream_group_leave, .says = "groups have no join or leave" },
        .{ .op = .stream_group_claim, .says = "groups have no claim: a read takes expired and nacked records" },
        .{ .op = .stream_group_configure_sweeper, .says = "set when it is created" },
    };
    for (cases) |c| {
        var r = try conn.sendRequest(c.op, "default", "s", "\x01\x00g");
        defer r.deinit();
        try testing.expect(r.isError());
        if (std.mem.indexOf(u8, r.errorMessage(), c.says) == null) std.debug.print("{s}: {s}\n", .{ @tagName(c.op), r.errorMessage() });
        try testing.expect(std.mem.indexOf(u8, r.errorMessage(), c.says) != null);
    }
}
