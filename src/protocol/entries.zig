//! The entry table's types: each log entry's payload declared as a schema,
//! like an op's request. Entries are durable, so a hand-written codec here
//! is the most dangerous kind to leave; this is where they move.
//!
//! It is a table of its own because entries aren't one-to-one with ops (a
//! transaction commit, a lease, a checkpoint). The table hash covers it, so
//! a data directory or a node built from another layout is refused.

const std = @import("std");
const schema = @import("schema.zig");

pub const Entry = struct {
    /// e.g. "kv.put": the entry's name in logs and metrics.
    name: []const u8,
    /// Its UAL entry type, stable once assigned.
    type_code: u8,
    /// The payload: a `schema.Record`.
    payload: type,
    /// The most this entry's payload may hold: the bound its proposer
    /// enforces. A family that proposes through `persistence.proposeEntry`
    /// gets `PROPOSE_ENTRY_PAYLOAD`; one that builds its own entries (KV)
    /// declares its own.
    max_payload: usize,
    doc: []const u8,
};

const limits = @import("limits.zig");

/// The payload room `persistence.proposeEntry` leaves: its buffer less the
/// command header and the longest key.
pub const PROPOSE_ENTRY_PAYLOAD: usize = limits.MAX_PERSIST_PAYLOAD - limits.COMMAND_PREFIX_SIZE - limits.MAX_QUALIFIED_KEY;

/// Checks a table at comptime, failing the build with a message naming the
/// entry and what's wrong.
pub fn check(comptime table: []const Entry) void {
    if (comptime tableRule(table)) |message| @compileError(message);
}

/// What's wrong with a table, or null: `check`'s rules, callable from tests.
pub fn tableRule(comptime table: []const Entry) ?[]const u8 {
    for (table, 0..) |e, i| {
        const where = "entry '" ++ e.name ++ "': ";
        for (table[0..i]) |other| {
            if (other.type_code == e.type_code) return std.fmt.comptimePrint("{s}type {d} is also '{s}'", .{ where, e.type_code, other.name });
            if (std.mem.eql(u8, other.name, e.name)) return where ++ "declared twice";
        }
        if (e.payload.kind != .record) return where ++ "payload is not a schema.Record";
        if (schema.fitsRule(e.payload, e.max_payload, where ++ "payload")) |m| return m;
        if (e.doc.len == 0) return where ++ "has no doc line";
    }
    return null;
}

const testing = std.testing;

test "entries: a table that follows the rules passes, and a payload decodes whole" {
    const table = [_]Entry{.{
        .name = "test.put",
        .type_code = 200,
        .payload = schema.Record(&.{
            .{ .name = "value", .T = schema.Bytes(1024) },
            .{ .name = "ttl_ms", .T = schema.Optional(schema.DurationMs(1 << 40)) },
        }),
        .max_payload = PROPOSE_ENTRY_PAYLOAD,
        .doc = "A test entry.",
    }};
    comptime check(&table);
    var buf: [64]u8 = undefined;
    const bytes = try table[0].payload.encodeInto(&buf, .{ .value = "v", .ttl_ms = 5 });
    var diag: schema.Diagnostic = .{};
    const v = try table[0].payload.decodeAll(bytes, &diag);
    try testing.expectEqualStrings("v", v.value);
    try testing.expectEqual(@as(?u64, 5), v.ttl_ms);
}

test "entries: a payload is checked against its own entry's bound" {
    const P = schema.Record(&.{.{ .name = "value", .T = schema.Bytes(100) }});
    const fits = [_]Entry{.{ .name = "t.a", .type_code = 1, .payload = P, .max_payload = 101, .doc = "x" }};
    const over = [_]Entry{.{ .name = "t.a", .type_code = 1, .payload = P, .max_payload = 100, .doc = "x" }};
    const twice = [_]Entry{ fits[0], .{ .name = "t.b", .type_code = 1, .payload = P, .max_payload = 101, .doc = "x" } };
    try testing.expectEqual(@as(?[]const u8, null), comptime tableRule(&fits));
    try testing.expect(comptime tableRule(&over) != null);
    try testing.expect(comptime tableRule(&twice) != null);
}
