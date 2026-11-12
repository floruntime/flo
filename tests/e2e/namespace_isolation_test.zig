//! Two namespaces holding a stream, a consumer group, a queue, a
//! measurement, a workflow or a processing job of the same name keep them
//! apart: each is keyed by the namespace's id and its name, from a request
//! and from the log alike, so a read in one never sees the other's records,
//! messages, points, runs or jobs, on one shard or many, and after a
//! restart replays the log. The two namespaces are a pair whose names once
//! hashed to the same 32 bits.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const colliding = @import("helpers/colliding_namespaces.zig");

const NAMESPACES = [_][]const u8{ colliding.a, colliding.b };

/// How many records, messages and points namespace `i` writes. The two
/// differ, so a read that merged them, or answered from the wrong one,
/// shows it in a count as well as in the payloads.
fn count(i: usize) usize {
    return i + 2;
}

/// What the CLI prints for `args`, its exit code checked.
fn run(ctx: *stdx.testing.TestContext, args: []const []const u8, want_exit: u8) ![]const u8 {
    var r = try ctx.cli.run(args);
    defer r.deinit();
    if (r.exit_code != want_exit) {
        std.debug.print("{s} {s}: exit {d}, wanted {d}\n  stdout: {s}\n  stderr: {s}\n", .{ args[0], args[1], r.exit_code, want_exit, r.stdout, r.stderr });
        return error.UnexpectedExit;
    }
    return testing.allocator.dupe(u8, std.mem.trimEnd(u8, r.stdout, "\n"));
}

fn parse(comptime T: type, text: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, testing.allocator, text, .{ .ignore_unknown_fields = true });
}

fn payload(buf: []u8, ns: []const u8, k: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d}", .{ ns, k });
}

/// Start a server, write with `write`, check with `verify`, restart, and
/// check again: the log rebuilds each namespace's state apart.
fn isolated(
    shards: u8,
    comptime write: fn (*stdx.testing.TestContext) anyerror!void,
    comptime verify: fn (*stdx.testing.TestContext, restarted: bool) anyerror!void,
) !void {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = shards, .durability = .sync } });
    defer ctx.deinit();
    try write(ctx);
    try verify(ctx, false);
    try ctx.restartServer();
    try verify(ctx, true);
}

// ── Streams ──────────────────────────────────────────────────────────────────

fn writeStreams(ctx: *stdx.testing.TestContext) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for (1..count(i) + 1) |k| try ctx.exec(&.{ "stream", "append", "same", try payload(&buf, ns, k), "-n", ns });
    }
}

fn verifyStreams(ctx: *stdx.testing.TestContext, _: bool) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const read = try run(ctx, &.{ "stream", "read", "same", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(read);
        const records = try parse([]const struct { data: []const u8 }, read);
        defer records.deinit();
        try testing.expectEqual(count(i), records.value.len);
        for (records.value, 1..) |r, k| try testing.expectEqualStrings(try payload(&buf, ns, k), r.data);

        const info = try run(ctx, &.{ "stream", "info", "same", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(info);
        const parsed = try parse(struct { namespace: []const u8, message_count: u64 }, info);
        defer parsed.deinit();
        try testing.expectEqualStrings(ns, parsed.value.namespace);
        try testing.expectEqual(count(i), parsed.value.message_count);
    }
}

// ── Consumer groups and stream settings ─────────────────────────────────────

const GroupRecord = struct { id: []const u8, data: []const u8 };

/// Each namespace's group reads its first two records and acks the first,
/// leaving one pending; and creates "cfg" with its own retention, then
/// alters it.
fn writeGroups(ctx: *stdx.testing.TestContext) !void {
    var buf: [64]u8 = undefined;
    var hours: [8]u8 = undefined;
    var keep: [8]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for (1..count(i) + 1) |k| try ctx.exec(&.{ "stream", "append", "same", try payload(&buf, ns, k), "-n", ns });
        try ctx.exec(&.{ "stream", "group", "create", "same", "--group", "g", "-n", ns });
        var ids: [2][]const u8 = undefined;
        for (1..3, &ids) |k, *id| {
            const read = try run(ctx, &.{ "stream", "group", "read", "same", "--group", "g", "--consumer", "c", "--limit", "1", "-o", "json", "-n", ns }, 0);
            defer testing.allocator.free(read);
            const records = try parse([]const GroupRecord, read);
            defer records.deinit();
            try testing.expectEqual(1, records.value.len);
            try testing.expectEqualStrings(try payload(&buf, ns, k), records.value[0].data);
            id.* = try testing.allocator.dupe(u8, records.value[0].id);
        }
        defer for (ids) |id| testing.allocator.free(id);
        const acked = try run(ctx, &.{ "stream", "group", "ack", "same", "--group", "g", "--consumer", "c", "--ids", ids[0], "-n", ns }, 0);
        defer testing.allocator.free(acked);
        try testing.expectEqualStrings("Acknowledged 1 message(s)", acked);

        try ctx.exec(&.{ "stream", "create", "cfg", "--retention", try std.fmt.bufPrint(&hours, "{d}", .{24 + i}), "-n", ns });
        try ctx.exec(&.{ "stream", "alter", "cfg", "--retention-count", try std.fmt.bufPrint(&keep, "{d}", .{10 + i}), "-n", ns });
    }
}

/// Before the restart each group has its member and the one pending
/// record; after it, the group and its cursor (past the acked record) are
/// what the log rebuilt, and its next read is its own namespace's rest.
/// The settings are each namespace's own either way.
fn verifyGroups(ctx: *stdx.testing.TestContext, restarted: bool) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const listed = try run(ctx, &.{ "stream", "group", "list", "same", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const groups = try parse(struct { groups: []const struct { name: []const u8, members: u32, pending: u64 }, more: bool }, listed);
        defer groups.deinit();
        try testing.expectEqual(1, groups.value.groups.len);
        try testing.expectEqualStrings("g", groups.value.groups[0].name);

        const info = try run(ctx, &.{ "stream", "info", "cfg", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(info);
        const settings = try parse(struct { namespace: []const u8, retention: struct { age_s: u64, count: u64 } }, info);
        defer settings.deinit();
        try testing.expectEqualStrings(ns, settings.value.namespace);
        try testing.expectEqual((24 + i) * 3600, settings.value.retention.age_s);
        try testing.expectEqual(10 + i, settings.value.retention.count);

        if (!restarted) {
            try testing.expectEqual(1, groups.value.groups[0].members);
            try testing.expectEqual(1, groups.value.groups[0].pending);
            const pending = try run(ctx, &.{ "stream", "group", "pending", "same", "--group", "g", "-o", "json", "-n", ns }, 0);
            defer testing.allocator.free(pending);
            const pel = try parse(struct { count: u64, pending: []const struct { consumer: []const u8 } }, pending);
            defer pel.deinit();
            try testing.expectEqual(1, pel.value.count);
            try testing.expectEqualStrings("c", pel.value.pending[0].consumer);
            const members = try run(ctx, &.{ "stream", "group", "members", "same", "g", "-o", "json", "-n", ns }, 0);
            defer testing.allocator.free(members);
            const m = try parse(struct { members: []const struct { id: []const u8, pending: u64 } }, members);
            defer m.deinit();
            try testing.expectEqual(1, m.value.members.len);
            try testing.expectEqualStrings("c", m.value.members[0].id);
            try testing.expectEqual(1, m.value.members[0].pending);
            continue;
        }
        const rest = try run(ctx, &.{ "stream", "group", "read", "same", "--group", "g", "--consumer", "c", "--limit", "16", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(rest);
        const records = try parse([]const GroupRecord, rest);
        defer records.deinit();
        try testing.expectEqual(count(i) - 1, records.value.len);
        for (records.value, 2..) |r, k| try testing.expectEqualStrings(try payload(&buf, ns, k), r.data);
    }
}

// ── Stream lists ─────────────────────────────────────────────────────────────

const Listed = struct { name: []const u8 };

/// Namespaces "a" and "ab": one's name is the other's prefix, so "a"'s
/// stream "bx" and "ab"'s stream "x" spell the same bytes after their
/// namespace's name. Keys that lead with the fixed-width id keep them
/// apart.
fn writePrefixed(ctx: *stdx.testing.TestContext) !void {
    for ([_][]const u8{ "x", "y" }) |s| try ctx.exec(&.{ "stream", "append", s, "one", "-n", "a" });
    try ctx.exec(&.{ "stream", "create", "bx", "-n", "a" });
    for ([_][]const u8{ "x", "z" }) |s| try ctx.exec(&.{ "stream", "append", s, "one", "-n", "ab" });
}

fn verifyPrefixed(ctx: *stdx.testing.TestContext, _: bool) !void {
    const cases = [_]struct { ns: []const u8, want: []const []const u8 }{
        .{ .ns = "a", .want = &.{ "bx", "x", "y" } },
        .{ .ns = "ab", .want = &.{ "x", "z" } },
        .{ .ns = "default", .want = &.{} },
    };
    for (cases) |c| {
        const listed = try run(ctx, &.{ "stream", "list", "-o", "json", "-n", c.ns }, 0);
        defer testing.allocator.free(listed);
        const streams = try parse([]const Listed, listed);
        defer streams.deinit();
        var got: [4][]const u8 = undefined;
        if (streams.value.len != c.want.len) std.debug.print("namespace {s}: {s}\n", .{ c.ns, listed });
        try testing.expectEqual(c.want.len, streams.value.len);
        for (streams.value, 0..) |s, j| got[j] = s.name;
        std.mem.sort([]const u8, got[0..c.want.len], {}, lessThanStr);
        for (c.want, got[0..c.want.len]) |w, g| try testing.expectEqualStrings(w, g);
    }
}

// ── Queues ───────────────────────────────────────────────────────────────────

const QueueStats = struct { name: []const u8, namespace: []const u8, ready: u64, enqueued: u64, dequeued: u64 };

fn writeQueues(ctx: *stdx.testing.TestContext) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for (1..count(i) + 1) |k| try ctx.exec(&.{ "queue", "enqueue", "same", try payload(&buf, ns, k), "-n", ns });
    }
}

/// Before the restart each namespace takes its first message; after it, the
/// rest. Either way the stats and what's dequeued are its own.
fn verifyQueues(ctx: *stdx.testing.TestContext, restarted: bool) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const taken: usize = @intFromBool(restarted);
        const listed = try run(ctx, &.{ "queue", "list", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const stats = try parse([]const QueueStats, listed);
        defer stats.deinit();
        try testing.expectEqual(1, stats.value.len);
        const s = stats.value[0];
        try testing.expectEqualStrings("same", s.name);
        try testing.expectEqualStrings(ns, s.namespace);
        try testing.expectEqual(count(i), s.enqueued);
        try testing.expectEqual(taken, s.dequeued);
        try testing.expectEqual(count(i) - taken, s.ready);

        if (!restarted) {
            const first = try run(ctx, &.{ "queue", "dequeue", "same", "--ack", "-o", "raw", "-n", ns }, 0);
            defer testing.allocator.free(first);
            try testing.expectEqualStrings(try payload(&buf, ns, 1), first);
            continue;
        }
        const rest = try run(ctx, &.{ "queue", "dequeue", "same", "--count", "16", "--ack", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(rest);
        var lines = std.mem.splitScalar(u8, rest, '\n');
        for (2..count(i) + 1) |k| try testing.expectEqualStrings(try payload(&buf, ns, k), lines.next() orelse return error.TooFewMessages);
        try testing.expect(lines.next() == null);
        const empty = try run(ctx, &.{ "queue", "dequeue", "same", "--ack", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(empty);
        try testing.expectEqualStrings("(no messages)", empty);
    }
}

// ── Time series ──────────────────────────────────────────────────────────────

fn writeSeries(ctx: *stdx.testing.TestContext) !void {
    var tags: [64]u8 = undefined;
    var ts: [32]u8 = undefined;
    var only: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const host = try std.fmt.bufPrint(&tags, "host={s}", .{ns});
        for (1..count(i) + 1) |k| {
            try ctx.exec(&.{ "ts", "write", "same", "--tags", host, "--value", "1", "--timestamp", try std.fmt.bufPrint(&ts, "{d}", .{1000 + k}), "-n", ns });
        }
        try ctx.exec(&.{ "ts", "write", try std.fmt.bufPrint(&only, "only_{s}", .{ns}), "--value", "1", "--timestamp", "1000", "-n", ns });
        // The same measurement and tag set in both: a delete by tag in the
        // first matches only its own namespace's series (`inMeasurement`
        // checks the key's id), so the second's stay.
        for (1..count(i) + 1) |k| {
            try ctx.exec(&.{ "ts", "write", "shared", "--tags", "site=x", "--value", "1", "--timestamp", try std.fmt.bufPrint(&ts, "{d}", .{1000 + k}), "-n", ns });
        }
    }
    try ctx.exec(&.{ "ts", "delete", "shared", "--tags", "site=x", "--confirm", "-n", NAMESPACES[0] });
}

fn verifySeries(ctx: *stdx.testing.TestContext, _: bool) !void {
    var only: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        // One series, this namespace's, counting this namespace's points.
        const queried = try run(ctx, &.{ "ts", "query", "same", "--from", "1", "--to", "100000", "--window", "1h", "--agg", "count", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(queried);
        var want: [32]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "0 {d}.000000", .{count(i)}), queried);

        const tagged = try run(ctx, &.{ "ts", "query", "same", "--tags", try std.fmt.bufPrint(&want, "host={s}", .{ns}), "--from", "1", "--to", "100000", "--window", "1h", "--agg", "count", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(tagged);
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "0 {d}.000000", .{count(i)}), tagged);

        // A listing: this namespace's measurements, and no other's; the
        // first namespace's "shared" is deleted, the second's is not.
        const listed = try run(ctx, &.{ "ts", "list", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const names = try parse([]const []const u8, listed);
        defer names.deinit();
        const mine = try std.fmt.bufPrint(&only, "only_{s}", .{ns});
        var want_names: [3][]const u8 = .{ mine, "same", "shared" };
        const n_want: usize = if (i == 0) 2 else 3;
        var got: [3][]const u8 = undefined;
        try testing.expectEqual(n_want, names.value.len);
        @memcpy(got[0..n_want], names.value);
        std.mem.sort([]const u8, got[0..n_want], {}, lessThanStr);
        std.mem.sort([]const u8, want_names[0..n_want], {}, lessThanStr);
        for (want_names[0..n_want], got[0..n_want]) |w, g| try testing.expectEqualStrings(w, g);

        if (i == 1) {
            const shared = try run(ctx, &.{ "ts", "query", "shared", "--tags", "site=x", "--from", "1", "--to", "100000", "--window", "1h", "--agg", "count", "-o", "raw", "-n", ns }, 0);
            defer testing.allocator.free(shared);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "0 {d}.000000", .{count(i)}), shared);
        }
    }
}

// ── Workflows ────────────────────────────────────────────────────────────────

/// A workflow whose runs wait for a signal, so they stay where they are
/// until one comes; version 1.0.<i> in namespace i.
fn workflowYaml(buf: []u8, name: []const u8, i: usize, schedule: bool) ![]const u8 {
    return std.fmt.bufPrint(buf,
        \\kind: Workflow
        \\name: {s}
        \\version: 1.0.{d}
        \\{s}start:
        \\  wait_for_signal:
        \\    type: go
        \\  transitions:
        \\    success: flo.Completed
        \\
    , .{ name, i, if (schedule) "schedule:\n  interval: 200\n  max_concurrent: 1\n" else "" });
}

fn runInput(buf: []u8, ns: []const u8, k: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "{{\"ns\":\"{s}\",\"k\":{d}}}", .{ ns, k });
}

const RunRow = struct { run_id: []const u8, workflow: []const u8, status: []const u8 };

/// The runs of `workflow` in `ns`, as list-runs prints them.
fn listRuns(ctx: *stdx.testing.TestContext, workflow: []const u8, ns: []const u8) !std.json.Parsed([]const RunRow) {
    const listed = try run(ctx, &.{ "workflow", "list-runs", "--workflow", workflow, "-o", "json", "-n", ns }, 0);
    defer testing.allocator.free(listed);
    // Copied out: the text is freed here.
    return std.json.parseFromSlice([]const RunRow, testing.allocator, listed, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
}

/// The line of `text` that starts with `label`, whole.
fn line(text: []const u8, label: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |l| if (std.mem.startsWith(u8, l, label)) return l;
    return null;
}

/// In each namespace: workflow "same" (its own version) with count(i)
/// runs, the first by the run id both namespaces choose, and "tick", a
/// schedule held at one run. Then namespace 0's chosen run is signalled.
fn writeWorkflows(ctx: *stdx.testing.TestContext) !void {
    var yaml: [512]u8 = undefined;
    var file: [64]u8 = undefined;
    var input: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for ([_][]const u8{ "same", "tick" }) |name| {
            const path = try stdx.testing.writeTempYaml(testing.allocator, try workflowYaml(&yaml, name, i, std.mem.eql(u8, name, "tick")), try std.fmt.bufPrint(&file, "ns-iso-{s}-{s}.yaml", .{ name, ns }));
            defer stdx.testing.cleanupTempFile(testing.allocator, path);
            try ctx.exec(&.{ "workflow", "create", "-f", path, "-n", ns });
        }
        for (1..count(i) + 1) |k| {
            const in = try runInput(&input, ns, k);
            const started = if (k == 1)
                try run(ctx, &.{ "workflow", "start", "same", in, "--run-id", "chosen", "-o", "json", "-n", ns }, 0)
            else
                try run(ctx, &.{ "workflow", "start", "same", in, "-o", "json", "-n", ns }, 0);
            defer testing.allocator.free(started);
            if (k == 1) try testing.expectEqualStrings("{\"run_id\":\"chosen\"}", started);
        }
    }
    const signalled = try run(ctx, &.{ "workflow", "signal", "chosen", "--type", "go", "-n", NAMESPACES[0] }, 0);
    defer testing.allocator.free(signalled);
    try testing.expectEqualStrings("Signal 'go' sent to workflow chosen", signalled);
    // The schedule's first slot: one run in each namespace, then no more.
    for (NAMESPACES) |ns| {
        var waited: usize = 0;
        while (true) : (waited += 1) {
            const ticks = try listRuns(ctx, "tick", ns);
            defer ticks.deinit();
            if (ticks.value.len > 0) break;
            if (waited == 50) return error.ScheduleNeverFired;
            stdx.time.sleep(100 * std.time.ns_per_ms);
        }
    }
    stdx.time.sleep(600 * std.time.ns_per_ms);
}

fn verifyWorkflows(ctx: *stdx.testing.TestContext, restarted: bool) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        // Its own two definitions, at its own version.
        const listed = try run(ctx, &.{ "workflow", "list", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const defs = try parse([]const struct { name: []const u8, version: []const u8 }, listed);
        defer defs.deinit();
        try testing.expectEqual(2, defs.value.len);
        for (defs.value) |d| {
            try testing.expect(std.mem.eql(u8, d.name, "same") or std.mem.eql(u8, d.name, "tick"));
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "1.0.{d}", .{i}), d.version);
        }

        // Its own runs, and the schedule held at one in each.
        const runs = try listRuns(ctx, "same", ns);
        defer runs.deinit();
        try testing.expectEqual(count(i), runs.value.len);
        var chosen: usize = 0;
        for (runs.value) |r| {
            try testing.expectEqualStrings("same", r.workflow);
            chosen += @intFromBool(std.mem.eql(u8, r.run_id, "chosen"));
        }
        try testing.expectEqual(1, chosen);
        const ticks = try listRuns(ctx, "tick", ns);
        defer ticks.deinit();
        try testing.expectEqual(1, ticks.value.len);

        // The run both chose: this namespace's input, and only namespace
        // 0's took the signal.
        const status = try run(ctx, &.{ "workflow", "status", "chosen", "-n", ns }, 0);
        defer testing.allocator.free(status);
        var want: [96]u8 = undefined;
        try testing.expectEqualStrings("Workflow: same (latest)", line(status, "Workflow:") orelse return error.NoLine);
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "Input:    {s}", .{try runInput(&buf, ns, 1)}), line(status, "Input:") orelse return error.NoLine);
        // A run's first step isn't taken again after a restart: one that
        // waited reads as running at its start.
        const want_status = if (i == 0) "completed" else if (restarted) "running" else "waiting";
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "Status:   {s}", .{want_status}), line(status, "Status:") orelse return error.NoLine);

        const history = try run(ctx, &.{ "workflow", "history", "chosen", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(history);
        const events = try parse([]const struct { type: []const u8, detail: []const u8 }, history);
        defer events.deinit();
        try testing.expectEqualStrings("workflow_started", events.value[0].type);
        try testing.expectEqualStrings(try runInput(&buf, ns, 1), events.value[0].detail);
    }
}

// ── Processing jobs ──────────────────────────────────────────────────────────

/// The jobs each namespace submitted, by its index.
var job_ids: [NAMESPACES.len][3][32]u8 = undefined;
var job_id_lens: [NAMESPACES.len][3]usize = undefined;

fn jobId(i: usize, k: usize) []const u8 {
    return job_ids[i][k][0..job_id_lens[i][k]];
}

/// count(i) jobs named "same" in each namespace, the first then stopped.
fn writeJobs(ctx: *stdx.testing.TestContext) !void {
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator,
        \\kind: Processing
        \\name: same
        \\sources.[0].stream.name: in
        \\sinks.[0].name: out
        \\sinks.[0].kv.key_prefix: out
    , "ns-iso-job.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    for (NAMESPACES, 0..) |ns, i| {
        for (0..count(i)) |k| {
            const submitted = try run(ctx, &.{ "processing", "submit", path, "-n", ns }, 0);
            defer testing.allocator.free(submitted);
            const prefix = "Job submitted: ";
            if (!std.mem.startsWith(u8, submitted, prefix)) return error.NoJobId;
            const id = submitted[prefix.len..];
            @memcpy(job_ids[i][k][0..id.len], id);
            job_id_lens[i][k] = id.len;
        }
        var want: [64]u8 = undefined;
        const stopped = try run(ctx, &.{ "processing", "stop", jobId(i, 0), "-n", ns }, 0);
        defer testing.allocator.free(stopped);
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "Job stopped: {s}", .{jobId(i, 0)}), stopped);
    }
}

fn verifyJobs(ctx: *stdx.testing.TestContext, restarted: bool) !void {
    var want: [96]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const other = 1 - i;
        const listed = try run(ctx, &.{ "processing", "list", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const jobs = try parse([]const struct { name: []const u8, job_id: []const u8, status: []const u8 }, listed);
        defer jobs.deinit();
        try testing.expectEqual(count(i), jobs.value.len);
        for (jobs.value) |j| {
            try testing.expectEqualStrings("same", j.name);
            var k: ?usize = null;
            for (0..count(i)) |n| if (std.mem.eql(u8, jobId(i, n), j.job_id)) {
                k = n;
            };
            try testing.expectEqualStrings(if ((k orelse return error.NotOwnJob) == 0) "STOPPED" else "RUNNING", j.status);
        }

        const status = try run(ctx, &.{ "processing", "status", jobId(i, 0), "-n", ns }, 0);
        defer testing.allocator.free(status);
        try testing.expectEqualStrings("Name:       same", line(status, "Name:") orelse return error.NoLine);
        try testing.expectEqualStrings("Status:     STOPPED", line(status, "Status:") orelse return error.NoLine);

        // The other namespace's jobs are not found from here, to read or
        // to stop.
        const theirs = try run(ctx, &.{ "processing", "status", jobId(other, 1), "-n", ns }, 1);
        testing.allocator.free(theirs);
        if (!restarted) {
            const stop = try run(ctx, &.{ "processing", "stop", jobId(other, 1), "-n", ns }, 1);
            testing.allocator.free(stop);
        }
        const still = try run(ctx, &.{ "processing", "status", jobId(other, 1), "-n", NAMESPACES[other] }, 0);
        defer testing.allocator.free(still);
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "Status:     {s}", .{"RUNNING"}), line(still, "Status:") orelse return error.NoLine);
    }
}

fn lessThanStr(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

test "e2e/namespace/isolation: the pair the tests use once collided on the 32-bit hash" {
    const old = struct {
        fn hash(name: []const u8) u32 {
            return @truncate(std.hash.Wyhash.hash(0, name));
        }
    }.hash;
    try testing.expect(!std.mem.eql(u8, colliding.a, colliding.b));
    try testing.expectEqual(old(colliding.a), old(colliding.b));
}

test "e2e/stream/isolation: a stream named in two namespaces reads and reports only its own records, on 1 shard and after a restart" {
    try isolated(1, writeStreams, verifyStreams);
}

test "e2e/stream/isolation: a stream named in two namespaces reads and reports only its own records, on 4 shards and after a restart" {
    try isolated(4, writeStreams, verifyStreams);
}

test "e2e/queue/isolation: a queue named in two namespaces dequeues and counts only its own messages, on 1 shard and after a restart" {
    try isolated(1, writeQueues, verifyQueues);
}

test "e2e/queue/isolation: a queue named in two namespaces dequeues and counts only its own messages, on 4 shards and after a restart" {
    try isolated(4, writeQueues, verifyQueues);
}

test "e2e/ts/isolation: a measurement named in two namespaces queries and lists only its own points, on 1 shard and after a restart" {
    try isolated(1, writeSeries, verifySeries);
}

test "e2e/ts/isolation: a measurement named in two namespaces queries and lists only its own points, on 4 shards and after a restart" {
    try isolated(4, writeSeries, verifySeries);
}

test "e2e/stream/isolation: a consumer group and a stream's settings named in two namespaces are each its own, on 1 shard and after a restart" {
    try isolated(1, writeGroups, verifyGroups);
}

test "e2e/stream/isolation: a consumer group and a stream's settings named in two namespaces are each its own, on 4 shards and after a restart" {
    try isolated(4, writeGroups, verifyGroups);
}

test "e2e/stream/isolation: namespaces \"a\" and \"ab\" each list only their own streams, on 1 shard and after a restart" {
    try isolated(1, writePrefixed, verifyPrefixed);
}

test "e2e/stream/isolation: namespaces \"a\" and \"ab\" each list only their own streams, on 4 shards and after a restart" {
    try isolated(4, writePrefixed, verifyPrefixed);
}

test "e2e/workflow/isolation: a workflow named in two namespaces lists, starts, signals and reports only its own runs, on 1 shard and after a restart" {
    try isolated(1, writeWorkflows, verifyWorkflows);
}

test "e2e/workflow/isolation: a workflow named in two namespaces lists, starts, signals and reports only its own runs, on 4 shards and after a restart" {
    try isolated(4, writeWorkflows, verifyWorkflows);
}

test "e2e/processing/isolation: jobs named in two namespaces list, stop and report only their own, on 1 shard and after a restart" {
    try isolated(1, writeJobs, verifyJobs);
}

test "e2e/processing/isolation: jobs named in two namespaces list, stop and report only their own, on 4 shards and after a restart" {
    try isolated(4, writeJobs, verifyJobs);
}
