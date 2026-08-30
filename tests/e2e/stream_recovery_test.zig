//! Stream Recovery E2E Tests
//!
//! Recovery must rebuild everything a restart depends on: a namespaced stream
//! stays visible to `stream list`, and `stream info` counts agree with what
//! `stream read` returns. Both are exercised against the shape that breaks
//! them — a namespaced stream fed by a batch append, across a restart.
//!
//! These assert on real output rather than exit status — the CLI exits 0 even
//! on protocol errors, so `assertSucceeded` alone proves nothing.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/stream: a namespaced stream is still listed after restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "namespace", "create", "myns" });
    try ctx.exec(&.{ "stream", "append", "events", "a", "b", "c", "-n", "myns" });

    var before = try ctx.cli.run(&.{ "stream", "list", "-n", "myns" });
    defer before.deinit();
    try testing.expect(std.mem.indexOf(u8, before.stdout, "events") != null);

    try ctx.restartServer();

    // `list` filters on the namespace prefix, so a stream registered under a
    // bare key vanishes from listings while staying resolvable by hash — which
    // is exactly how `info` and `read` can keep working when `list` does not.
    var after = try ctx.cli.run(&.{ "stream", "list", "-n", "myns" });
    defer after.deinit();
    try testing.expect(std.mem.indexOf(u8, after.stdout, "events") != null);
    try testing.expect(std.mem.indexOf(u8, after.stdout, "No streams found") == null);
}

test "e2e/stream: info counts match read across restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // One batch append carrying three records.
    try ctx.exec(&.{ "stream", "append", "counted", "a", "b", "c" });

    var read_before = try ctx.cli.run(&.{ "stream", "read", "counted" });
    defer read_before.deinit();
    try testing.expect(std.mem.indexOf(u8, read_before.stdout, ": a") != null);
    try testing.expect(std.mem.indexOf(u8, read_before.stdout, ": c") != null);

    // `info` counted append *entries*, so a 3-record batch reported 1, and the
    // size was hardcoded to 0.
    var info_before = try ctx.cli.run(&.{ "stream", "info", "counted" });
    defer info_before.deinit();
    try testing.expect(std.mem.indexOf(u8, info_before.stdout, "Records: 3") != null);
    try testing.expect(std.mem.indexOf(u8, info_before.stdout, "Size: 0 bytes") == null);

    try ctx.restartServer();

    var info_after = try ctx.cli.run(&.{ "stream", "info", "counted" });
    defer info_after.deinit();
    try testing.expect(std.mem.indexOf(u8, info_after.stdout, "Records: 3") != null);
    try testing.expect(std.mem.indexOf(u8, info_after.stdout, "Size: 0 bytes") == null);

    // Records still readable, and the same three.
    var read_after = try ctx.cli.run(&.{ "stream", "read", "counted" });
    defer read_after.deinit();
    try testing.expect(std.mem.indexOf(u8, read_after.stdout, ": a") != null);
    try testing.expect(std.mem.indexOf(u8, read_after.stdout, ": c") != null);
}

// NOTE: the same-millisecond StreamID-reuse case is covered by a projection
// unit test, not here: two CLI round-trips are ~60ms apart, so an e2e test
// could never land both appends in one millisecond and would pass whether or
// not the behaviour is correct.
