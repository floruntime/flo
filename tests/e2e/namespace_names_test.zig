//! A namespace name meets one rule, however it arrives: an explicit create,
//! or any request naming it, which creates it on a first write. Each test
//! checks the server's answer, and that nothing was created.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

fn listed(ctx: *stdx.testing.TestContext, name: []const u8) !bool {
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    return std.mem.indexOf(u8, list, name) != null;
}

test "e2e/namespace: a write naming a namespace outside the rule is refused and creates nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const long = "n" ** 300;
    for ([_][]const u8{ long, "_flo", "_mine", "a:b", "trailing-", "9lives" }) |name| {
        var put = try ctx.cli.run(&.{ "kv", "set", "k", "v", "-n", name });
        defer put.deinit();
        try stdx.testing.assertContains(put, "namespace name");
        var append = try ctx.cli.run(&.{ "stream", "append", "s", "x", "-n", name });
        defer append.deinit();
        try stdx.testing.assertContains(append, "namespace name");
        try testing.expect(!try listed(ctx, name));
    }
    // The longest name, and the old rule's dots, still work.
    const longest = "n" ** 63;
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", longest });
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", "v1.0" });
    try testing.expect(try listed(ctx, longest));
    try testing.expect(try listed(ctx, "v1.0"));
}

test "e2e/namespace: an explicit create meets the same rule" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    for ([_][]const u8{ "n" ** 64, "_flo", "trailing-" }) |name| {
        var create = try ctx.cli.run(&.{ "ns", "create", name });
        defer create.deinit();
        try stdx.testing.assertContains(create, "namespace name");
        try testing.expect(!try listed(ctx, name));
    }
}

test "e2e/namespace: force delete of a long-named namespace leaves the server serving" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const long = "n" ** 300;
    var append = try ctx.cli.run(&.{ "stream", "append", "s", "x", "-n", long });
    defer append.deinit();
    var delete = try ctx.cli.run(&.{ "ns", "delete", long, "--force" });
    defer delete.deinit();
    try stdx.testing.assertContains(delete, "namespace name");
    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    const got = try ctx.execCapture(&.{ "kv", "get", "alive" });
    try testing.expect(std.mem.indexOf(u8, got, "yes") != null);
}

test "e2e/namespace: force delete does not take other namespaces' streams and queues with it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "orders", "keep-1", "-n", "keep" });
    try ctx.exec(&.{ "queue", "enqueue", "jobs", "keep-2", "-n", "keep" });
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", "gone" });

    var delete = try ctx.cli.run(&.{ "ns", "delete", "gone", "--force" });
    defer delete.deinit();
    try stdx.testing.assertContains(delete, "cannot remove streams or queues yet");
    try testing.expect(try listed(ctx, "gone"));

    const info = try ctx.execCapture(&.{ "stream", "info", "orders", "-n", "keep" });
    try testing.expect(std.mem.indexOf(u8, info, "Records: 1") != null);
    const msg = try ctx.execCapture(&.{ "queue", "dequeue", "jobs", "-n", "keep", "--timeout", "100" });
    try testing.expect(std.mem.indexOf(u8, msg, "keep-2") != null);
}

test "e2e/namespace: writes cannot create namespaces past the limit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    // "default" is one of the 1024.
    try ctx.exec(&.{ "kv", "set", "k", "v" });
    var buf: [16]u8 = undefined;
    for (1..1024) |i| {
        try ctx.exec(&.{ "kv", "set", "k", "v", "-n", try std.fmt.bufPrint(&buf, "ns{d}", .{i}) });
    }
    var over = try ctx.cli.run(&.{ "kv", "set", "k", "v", "-n", "one-more" });
    defer over.deinit();
    try stdx.testing.assertContains(over, "namespace limit reached");
    try testing.expect(!try listed(ctx, "one-more"));
    var create = try ctx.cli.run(&.{ "ns", "create", "one-more" });
    defer create.deinit();
    try stdx.testing.assertContains(create, "namespace limit reached");
    try testing.expect(!try listed(ctx, "one-more"));
    // A namespace that exists still takes writes.
    try ctx.exec(&.{ "kv", "set", "k2", "v", "-n", "ns7" });
}
