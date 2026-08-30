//! Stream Namespace Isolation E2E Tests
//!
//! Stream metadata — partition count and retention — must be per-namespace.
//! Sharing it across namespaces lets one tenant re-route or expire another
//! tenant's same-named stream, since partition count drives routing.
//!
//! Asserts on real output rather than exit status — the CLI exits 0 even on
//! protocol errors.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/stream/ns: partition count is per-namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "namespace", "create", "tenant-a" });
    try ctx.exec(&.{ "namespace", "create", "tenant-b" });

    try ctx.exec(&.{ "stream", "create", "shared", "--partitions", "4", "-n", "tenant-a" });

    // Creating the same name elsewhere must not touch tenant-a's config.
    try ctx.exec(&.{ "stream", "create", "shared", "--partitions", "1", "-n", "tenant-b" });

    var a = try ctx.cli.run(&.{ "stream", "info", "shared", "-n", "tenant-a" });
    defer a.deinit();
    try testing.expect(std.mem.indexOf(u8, a.stdout, "Partitions: 4") != null);

    var b = try ctx.cli.run(&.{ "stream", "info", "shared", "-n", "tenant-b" });
    defer b.deinit();
    try testing.expect(std.mem.indexOf(u8, b.stdout, "Partitions: 1") != null);
}

test "e2e/stream/ns: list reports each namespace's own partition count" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "namespace", "create", "tenant-a" });
    try ctx.exec(&.{ "namespace", "create", "tenant-b" });
    try ctx.exec(&.{ "stream", "create", "shared", "--partitions", "4", "-n", "tenant-a" });
    try ctx.exec(&.{ "stream", "create", "shared", "--partitions", "1", "-n", "tenant-b" });

    // `list` strips the namespace prefix before looking metadata back up, so it
    // needs the namespace to re-qualify — a separate path from `info`.
    var a = try ctx.cli.run(&.{ "stream", "list", "-n", "tenant-a" });
    defer a.deinit();
    try testing.expect(std.mem.indexOf(u8, a.stdout, "shared") != null);
    try testing.expect(std.mem.indexOf(u8, a.stdout, "4") != null);

    var b = try ctx.cli.run(&.{ "stream", "list", "-n", "tenant-b" });
    defer b.deinit();
    try testing.expect(std.mem.indexOf(u8, b.stdout, "shared") != null);
    try testing.expect(std.mem.indexOf(u8, b.stdout, "4") == null);
}

test "e2e/stream/ns: retention is per-namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "namespace", "create", "tenant-a" });
    try ctx.exec(&.{ "namespace", "create", "tenant-b" });
    try ctx.exec(&.{ "stream", "create", "shared", "-n", "tenant-a" });
    try ctx.exec(&.{ "stream", "create", "shared", "-n", "tenant-b" });

    // Shared metadata would let one namespace shorten another's retention,
    // silently deleting the other tenant's data.
    try ctx.exec(&.{ "stream", "alter", "shared", "--retention", "24", "-n", "tenant-a" });

    var a = try ctx.cli.run(&.{ "stream", "info", "shared", "-n", "tenant-a" });
    defer a.deinit();
    try testing.expect(std.mem.indexOf(u8, a.stdout, "86400s") != null);

    var b = try ctx.cli.run(&.{ "stream", "info", "shared", "-n", "tenant-b" });
    defer b.deinit();
    try testing.expect(std.mem.indexOf(u8, b.stdout, "86400s") == null);
    try testing.expect(std.mem.indexOf(u8, b.stdout, "Retention") == null);
}
