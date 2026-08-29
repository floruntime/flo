//! Stream Namespace Isolation E2E Tests (issue #46)
//!
//! Stream metadata (partition count + retention) was keyed by the bare stream
//! name, so a same-named stream in a second namespace silently overwrote the
//! first one's configuration. Partition count drives routing, so one tenant
//! could re-route another tenant's stream just by picking the same name.
//!
//! Asserts on real output rather than exit status — the CLI exits 0 even on
//! protocol errors.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/stream/ns: partition count is per-namespace (#46)" {
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

test "e2e/stream/ns: list reports each namespace's own partition count (#46)" {
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

test "e2e/stream/ns: retention is per-namespace (#46)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "namespace", "create", "tenant-a" });
    try ctx.exec(&.{ "namespace", "create", "tenant-b" });
    try ctx.exec(&.{ "stream", "create", "shared", "-n", "tenant-a" });
    try ctx.exec(&.{ "stream", "create", "shared", "-n", "tenant-b" });

    // Shared metadata meant one namespace could shorten another's retention,
    // which silently deletes the other tenant's data.
    try ctx.exec(&.{ "stream", "alter", "shared", "--retention", "24", "-n", "tenant-a" });

    var a = try ctx.cli.run(&.{ "stream", "info", "shared", "-n", "tenant-a" });
    defer a.deinit();
    try testing.expect(std.mem.indexOf(u8, a.stdout, "86400s") != null);

    var b = try ctx.cli.run(&.{ "stream", "info", "shared", "-n", "tenant-b" });
    defer b.deinit();
    try testing.expect(std.mem.indexOf(u8, b.stdout, "86400s") == null);
    try testing.expect(std.mem.indexOf(u8, b.stdout, "Retention") == null);
}
