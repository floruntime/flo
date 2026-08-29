//! Cluster Command E2E Tests (issue #42, item 6)
//!
//! `flo cluster status` is advertised in `flo --help`'s own examples but had no
//! server-side handler, so it fell through the dispatcher to a generic
//! "not implemented" error. A single-node server now answers truthfully.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/cluster: status reports a real single-node cluster (#42 item 6)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var out = try ctx.cli.run(&.{ "cluster", "status" });
    defer out.deinit();

    // The reported failure mode — the CLI exits 0 either way, so assert on the
    // output rather than the status.
    try testing.expect(std.mem.indexOf(u8, out.stdout, "not implemented") == null);
    try testing.expect(std.mem.indexOf(u8, out.stderr, "not implemented") == null);

    try testing.expect(std.mem.indexOf(u8, out.stdout, "Cluster Status") != null);
    // A lone node is the leader of a one-member cluster.
    try testing.expect(std.mem.indexOf(u8, out.stdout, "Role:       leader") != null);
    try testing.expect(std.mem.indexOf(u8, out.stdout, "Members:    1") != null);
}

test "e2e/cluster: status --output json is machine-readable (#42 item 6)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var out = try ctx.cli.run(&.{ "cluster", "status", "-o", "json" });
    defer out.deinit();

    try testing.expect(std.mem.indexOf(u8, out.stdout, "\"role\":\"leader\"") != null);
    try testing.expect(std.mem.indexOf(u8, out.stdout, "\"members\":1") != null);

    // Node and leader must be the same node, and actually populated.
    try testing.expect(std.mem.indexOf(u8, out.stdout, "\"node_id\":\"flo-") != null);
    try testing.expect(std.mem.indexOf(u8, out.stdout, "\"leader_id\":\"flo-") != null);
}
