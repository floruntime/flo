//! A blocking read waits for what it names by its namespace's id. A
//! namespace nobody created has no id, so nothing could ever wake the wait:
//! it is refused no_namespace at once, not answered empty when the block
//! runs out, and the namespace stays uncreated. In a namespace that exists
//! the same read still waits, and an append or an enqueue wakes it.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const time = stdx.time;

/// Long enough that a refusal sent only when it ran out is plain.
const BLOCK_MS = "30000";
/// A refusal sent at once comes well inside this.
const AT_ONCE_MS = 5_000;
/// How long a waiter is left before it is woken.
const PARK_MS = 500;

fn refusedAtOnce(ctx: *stdx.testing.TestContext, args: []const []const u8) !void {
    const t0 = time.monotonicMs();
    var r = try ctx.cli.run(args);
    defer r.deinit();
    const took = time.monotonicMs() - t0;
    if (r.exit_code != 1 or !r.stderrContains("[no_namespace/") or took >= AT_ONCE_MS) {
        std.debug.print("{s} {s}: exit {d} after {d} ms\n  stdout: {s}\n  stderr: {s}\n", .{ args[0], args[1], r.exit_code, took, r.stdout, r.stderr });
        return error.NotRefusedAtOnce;
    }
}

fn neverCreated(ctx: *stdx.testing.TestContext) !void {
    try refusedAtOnce(ctx, &.{ "stream", "group", "read", "events", "--group", "g", "--consumer", "c1", "--block", BLOCK_MS, "-n", "never-made" });
    try refusedAtOnce(ctx, &.{ "queue", "dequeue", "jobs", "--block", BLOCK_MS, "-n", "never-made" });
    // Refusing the wait made nothing.
    var listed = try ctx.cli.run(&.{ "namespace", "list", "-o", "json" });
    defer listed.deinit();
    try testing.expectEqual(0, listed.exit_code);
    try testing.expect(!listed.stdoutContains("never-made"));
}

/// Park `args`, then run `wake` after PARK_MS: the parked command must
/// have waited for it and ended soon after, long before its block ran out.
fn wokenBy(ctx: *stdx.testing.TestContext, args: []const []const u8, wake: []const []const u8) !stdx.testing.CommandResult {
    const t0 = time.monotonicMs();
    var waiter = try ctx.cli.runAsync(args);
    defer waiter.deinit();
    time.sleep(PARK_MS * std.time.ns_per_ms);
    try testing.expect(!waiter.completed.load(.acquire));
    try ctx.exec(wake);
    const r = try waiter.waitWithTimeout(AT_ONCE_MS);
    const took = time.monotonicMs() - t0;
    if (r.exit_code != 0 or took < PARK_MS or took >= PARK_MS + AT_ONCE_MS) {
        std.debug.print("{s} {s}: exit {d} after {d} ms\n  stdout: {s}\n  stderr: {s}\n", .{ args[0], args[1], r.exit_code, took, r.stdout, r.stderr });
        var owned = r;
        owned.deinit();
        return error.NotWoken;
    }
    return r;
}

fn existing(ctx: *stdx.testing.TestContext) !void {
    try ctx.exec(&.{ "namespace", "create", "made" });

    var dequeued = try wokenBy(ctx, &.{ "queue", "dequeue", "jobs", "--block", BLOCK_MS, "-o", "raw", "-n", "made" }, &.{ "queue", "enqueue", "jobs", "the-job", "-n", "made" });
    defer dequeued.deinit();
    try testing.expectEqualStrings("the-job", dequeued.stdoutTrimmed());

    // A woken group read is answered empty, and the reader reads again: the
    // record goes through the group's own read, which tracks it as pending.
    var woken = try wokenBy(ctx, &.{ "stream", "group", "read", "events", "--group", "g", "--consumer", "c1", "--block", BLOCK_MS, "-o", "json", "-n", "made" }, &.{ "stream", "append", "events", "the-event", "-n", "made" });
    defer woken.deinit();
    var again = try ctx.cli.run(&.{ "stream", "group", "read", "events", "--group", "g", "--consumer", "c1", "-o", "json", "-n", "made" });
    defer again.deinit();
    try testing.expectEqual(0, again.exit_code);
    try testing.expect(woken.stdoutContains("the-event") or again.stdoutContains("the-event"));
}

fn waits(shards: u8) !void {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = shards } });
    defer ctx.deinit();
    try neverCreated(ctx);
    try existing(ctx);
}

test "e2e/namespace/wait: a blocking group read or dequeue in a namespace never created is refused at once, and one in a namespace that exists is woken, on 1 shard" {
    try waits(1);
}

test "e2e/namespace/wait: a blocking group read or dequeue in a namespace never created is refused at once, and one in a namespace that exists is woken, on 4 shards" {
    try waits(4);
}
