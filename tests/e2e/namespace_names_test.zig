//! A namespace name meets one rule however it arrives: an explicit create,
//! any request naming it (a first write creates it), a pipeline definition,
//! a workflow trigger, or the dashboard. Each test checks the answer, and
//! that nothing was created or harmed.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

fn listed(ctx: *stdx.testing.TestContext, name: []const u8) !bool {
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    return std.mem.indexOf(u8, list, name) != null;
}

const Bad = struct { name: []const u8, says: []const u8 };
const bad_names = [_]Bad{
    .{ .name = "n" ** 300, .says = "namespace name too long" },
    .{ .name = "_flo", .says = "reserved namespace name" },
    .{ .name = "Prod", .says = "lowercase only" },
    .{ .name = "v1.0", .says = "letters, digits, '_' and '-' only" },
    .{ .name = "a:b", .says = "letters, digits, '_' and '-' only" },
    .{ .name = "trailing-", .says = "must not end with '-'" },
};

test "e2e/namespace: a write naming a namespace outside the rule is refused, says why, and creates nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    for (bad_names) |b| {
        var put = try ctx.cli.run(&.{ "kv", "set", "k", "v", "-n", b.name });
        defer put.deinit();
        try stdx.testing.assertStderrContains(put, b.says);
        var append = try ctx.cli.run(&.{ "stream", "append", "s", "x", "-n", b.name });
        defer append.deinit();
        try stdx.testing.assertStderrContains(append, b.says);
        try testing.expect(!try listed(ctx, b.name));
    }
    // The longest name, and one starting with a digit, are names.
    const longest = "n" ** 63;
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", longest });
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", "9lives" });
    try testing.expect(try listed(ctx, longest));
    try testing.expect(try listed(ctx, "9lives"));
}

test "e2e/namespace: an explicit create meets the same rule" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    for (bad_names) |b| {
        var create = try ctx.cli.run(&.{ "ns", "create", b.name });
        defer create.deinit();
        try stdx.testing.assertStderrContains(create, b.says);
        try testing.expect(!try listed(ctx, b.name));
    }
}

test "e2e/namespace: a pipeline naming a namespace outside the rule is refused at submit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const Case = struct { line: []const u8, says: []const u8 };
    for ([_]Case{
        .{ .line = "sinks.[0].stream.namespace: _flo", .says = "reserved namespace name" },
        .{ .line = "sources.[0].stream.namespace: Prod", .says = "lowercase only" },
        .{ .line = "namespace: has_colon:x", .says = "letters, digits, '_' and '-' only" },
    }) |c| {
        var buf: [512]u8 = undefined;
        const def = try std.fmt.bufPrint(&buf,
            \\kind: Processing
            \\name: ns-check
            \\sources.[0].stream.name: in
            \\sinks.[0].stream.name: out
            \\{s}
        , .{c.line});
        const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "ns-check.yaml");
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        var submit = try ctx.cli.run(&.{ "processing", "submit", path });
        defer submit.deinit();
        try stdx.testing.assertContains(submit, c.says);
    }
    const jobs = try ctx.execCapture(&.{ "processing", "list" });
    try testing.expect(std.mem.indexOf(u8, jobs, "ns-check") == null);
}

test "e2e/namespace: the dashboard refuses an invalid namespace name, and the node keeps serving" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    const long = "n" ** 5000;
    for ([_][]const u8{
        "/api/v1/streams?namespace=" ++ long,
        "/api/v1/kv/namespaces/" ++ long ++ "/keys",
        "/api/v1/namespaces/" ++ long,
    }) |path| {
        var r = try http.get(path);
        defer r.deinit();
        try testing.expect(r.bodyContains("namespace name too long"));
    }
    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "alive" }), "yes") != null);
}

test "e2e/namespace: writes cannot create namespaces past the limit, and the refusal says so" {
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
    try stdx.testing.assertStderrContains(over, "namespace limit reached (1024 per shard)");
    var create = try ctx.cli.run(&.{ "ns", "create", "one-more" });
    defer create.deinit();
    try stdx.testing.assertStderrContains(create, "namespace limit reached (1024 per shard)");
    // The listing shows at most 256 per shard, so ask for it by name.
    var info = try ctx.cli.run(&.{ "ns", "info", "one-more" });
    defer info.deinit();
    try stdx.testing.assertStdoutContains(info, "Namespace 'one-more' does not exist");
    // A namespace that exists still takes writes.
    try ctx.exec(&.{ "kv", "set", "k2", "v", "-n", "ns7" });
}

test "e2e/namespace: a workflow trigger naming a namespace outside the rule is refused at create" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "ns-echo" });
    const def =
        \\kind: Workflow
        \\name: ns-trigger
        \\version: 1.0.0
        \\trigger.stream: events
        \\trigger.namespace: _flo
        \\start.run: @actions/ns-echo
        \\start.transitions.success: flo.Completed
        \\start.transitions.failure: flo.Failed
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, def, "ns-trigger.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    var create = try ctx.cli.run(&.{ "workflow", "create", "-f", path });
    defer create.deinit();
    try stdx.testing.assertContains(create, "reserved namespace name");
    var got = try ctx.cli.run(&.{ "workflow", "definition", "ns-trigger" });
    defer got.deinit();
    try testing.expect(!got.stdoutContains("ns-trigger"));
}

test "e2e/namespace: a write that brings no data, to a namespace never seen, creates nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    var del = try ctx.cli.run(&.{ "kv", "delete", "k", "-n", "typo-a" });
    defer del.deinit();
    var deq = try ctx.cli.run(&.{ "queue", "dequeue", "q", "-n", "typo-b", "--timeout", "100" });
    defer deq.deinit();
    try stdx.testing.assertContains(deq, "(no messages)");
    var empty_key = try ctx.cli.run(&.{ "kv", "set", "", "v", "-n", "typo-c" });
    defer empty_key.deinit();
    for ([_][]const u8{ "typo-a", "typo-b", "typo-c" }) |name| {
        var info = try ctx.cli.run(&.{ "ns", "info", name });
        defer info.deinit();
        var buf: [64]u8 = undefined;
        try stdx.testing.assertStdoutContains(info, try std.fmt.bufPrint(&buf, "Namespace '{s}' does not exist", .{name}));
    }
}
