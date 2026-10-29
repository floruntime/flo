//! Golden vectors for the layouts still coded by hand: one checked-in byte
//! file per request or answer, which the encoder must produce and the
//! decoder must read back. Their bytes are folded into the table hash, so
//! changing a layout means changing its vector, and that moves the hash:
//! a client or node built before the change is refused, not misread.
//!
//! A hand-coded op with no vector yet is listed in `NO_VECTOR_YET`, which
//! only shrinks. A family leaves it by moving onto the op table, whose hash
//! covers its layouts already, or, where that isn't planned, by gaining
//! vectors here.

const std = @import("std");
const proto = @import("proto.zig");
const table = @import("table.zig");
const wire = @import("cluster_wire.zig");

pub const Vector = struct {
    op: proto.OpCode,
    name: []const u8,
    bytes: []const u8,
};

pub const vectors = [_]Vector{
    vector(.cluster_status, "cluster_status.ans"),
    vector(.cluster_members, "cluster_members.ans"),
    vector(.cluster_promote, "cluster_promote.req"),
    vector(.cluster_remove, "cluster_remove.req"),
    vector(.cluster_repair_skip, "cluster_repair_skip.req"),
    vector(.cluster_repair_skip, "cluster_repair_skip.ans"),
};

fn vector(comptime op: proto.OpCode, comptime name: []const u8) Vector {
    return .{ .op = op, .name = name, .bytes = @embedFile("golden/" ++ name ++ ".bin") };
}

/// Hand-coded ops whose layouts have no vector yet: while any remain, a
/// change to one of their layouts still needs `table.HAND_LAYOUT_EPOCH`
/// bumped by hand. Remove an op as it gains a vector or moves onto the op
/// table; never add one.
pub const NO_VECTOR_YET = [_]proto.OpCode{
    .ping,                    .namespace_create,     .namespace_delete,               .namespace_list,            .namespace_info,
    .namespace_config_set,    .namespace_config_get, .stream_append,                  .stream_read,               .stream_trim,
    .stream_info,             .stream_list,          .stream_create,                  .stream_alter,              .stream_delete,
    .stream_group_create,     .stream_group_join,    .stream_group_leave,             .stream_group_read,         .stream_group_ack,
    .stream_group_claim,      .stream_group_pending, .stream_group_configure_sweeper, .stream_group_nack,         .stream_group_touch,
    .stream_group_info,       .stream_group_delete,  .queue_enqueue,                  .queue_dequeue,             .queue_complete,
    .queue_fail,              .queue_dlq_list,       .queue_dlq_delete,               .queue_dlq_requeue,         .queue_stats,
    .queue_peek,              .queue_purge,          .queue_list,                     .ts_write,                  .ts_read,
    .ts_query,                .ts_floql,             .ts_list,                        .ts_delete,                 .ts_retention,
    .action_register,         .action_invoke,        .action_list,                    .action_list_runs,          .action_delete,
    .action_await,            .action_complete,      .action_fail,                    .action_touch,              .worker_register,
    .worker_heartbeat,        .worker_deregister,    .worker_drain,                   .workflow_create,           .workflow_start,
    .workflow_signal,         .workflow_cancel,      .workflow_status,                .workflow_history,          .workflow_list_runs,
    .workflow_get_definition, .workflow_disable,     .workflow_enable,                .workflow_list_definitions, .processing_submit,
    .processing_stop,         .processing_cancel,    .processing_status,              .processing_list,
};

/// The most `NO_VECTOR_YET` may hold: lower it as the list shrinks, so an
/// op added to the list fails the build.
const NO_VECTOR_YET_CAP = 74;

/// Every vector's name and bytes, for the table hash.
pub fn hash(h: *std.hash.Wyhash) void {
    for (vectors) |v| {
        h.update(v.name);
        h.update(std.mem.asBytes(&@as(u64, v.bytes.len)));
        h.update(v.bytes);
    }
}

const testing = std.testing;

const halt: wire.Halt = .{ .index = 987, .term = 5, .crc = 0xdeadbeef };

/// What each vector holds.
const status: wire.Status = .{ .node_id = 7, .leader_id = 3, .term = 42, .state = 2, .member_count = 3, .vouched_index = 0x1122, .strangers_while_quiet = true };
const members = [_]wire.Member{
    .{ .id = 3, .role = 0, .ip4 = .{ 10, 0, 0, 3 }, .port = 9000, .contact_ago_ms = wire.Member.UNKNOWN_CONTACT, .match = wire.Member.UNKNOWN_MATCH, .halt = .{ .at = halt } },
    .{ .id = 4, .role = 0, .ip4 = .{ 10, 0, 0, 4 }, .port = 9001, .contact_ago_ms = 12, .match = 990, .halt = .{ .at = halt } },
    .{ .id = 5, .role = 1, .ip4 = .{ 0, 0, 0, 0 }, .port = 0, .contact_ago_ms = wire.Member.UNKNOWN_CONTACT, .match = wire.Member.UNKNOWN_MATCH, .halt = .unknown },
};
const removed = [_]wire.Removed{.{ .id = 9, .when_ms = 1_700_000_000_000 }};
const promote: wire.Promote = .{ .id = 4 };
const remove: wire.Remove = .{ .id = 9, .yes = true };
const skip: wire.RepairSkip = .{ .index = 987, .yes = true, .reason = "malformed invoke" };
const skip_answer: wire.RepairSkipAnswer = .{ .skipped = true, .skip_index = 1002, .entry = halt, .entry_type = 0x30, .entry_size = 512, .voters = 3, .halted = &.{ 3, 4 } };

fn bytesOf(comptime name: []const u8) []const u8 {
    for (vectors) |v| if (std.mem.eql(u8, v.name, name)) return v.bytes;
    unreachable;
}

test "golden: each cluster layout encodes to its vector and decodes from it" {
    var sbuf: [wire.Status.SIZE]u8 = undefined;
    try testing.expectEqualSlices(u8, bytesOf("cluster_status.ans"), status.encode(&sbuf));
    try testing.expectEqualDeep(status, try wire.Status.decode(bytesOf("cluster_status.ans")));

    var mbuf: [wire.Members.maxSize(members.len, removed.len)]u8 = undefined;
    try testing.expectEqualSlices(u8, bytesOf("cluster_members.ans"), wire.Members.encode(&mbuf, 3, 1000, &members, &removed));
    const m = try wire.Members.decode(bytesOf("cluster_members.ans"));
    try testing.expectEqual(@as(u32, 3), m.leader);
    try testing.expectEqual(@as(u64, 1000), m.commit);
    try testing.expectEqual(members.len, m.count());
    for (members, 0..) |want, i| try testing.expectEqualDeep(want, try m.member(i));
    try testing.expectEqual(removed.len, m.removedCount());
    try testing.expectEqualDeep(removed[0], m.removed(0));

    var pbuf: [wire.Promote.SIZE]u8 = undefined;
    try testing.expectEqualSlices(u8, bytesOf("cluster_promote.req"), promote.encode(&pbuf));
    try testing.expectEqualDeep(promote, try wire.Promote.decode(bytesOf("cluster_promote.req")));

    var rbuf: [wire.Remove.SIZE]u8 = undefined;
    try testing.expectEqualSlices(u8, bytesOf("cluster_remove.req"), remove.encode(&rbuf));
    try testing.expectEqualDeep(remove, try wire.Remove.decode(bytesOf("cluster_remove.req")));

    var kbuf: [wire.RepairSkip.MAX_SIZE]u8 = undefined;
    try testing.expectEqualSlices(u8, bytesOf("cluster_repair_skip.req"), try skip.encode(&kbuf));
    try testing.expectEqualDeep(skip, try wire.RepairSkip.decode(bytesOf("cluster_repair_skip.req")));

    var abuf: [wire.RepairSkipAnswer.maxSize(2)]u8 = undefined;
    try testing.expectEqualSlices(u8, bytesOf("cluster_repair_skip.ans"), skip_answer.encode(&abuf));
    var ids: [8]u32 = undefined;
    try testing.expectEqualDeep(skip_answer, try wire.RepairSkipAnswer.decode(bytesOf("cluster_repair_skip.ans"), &ids));
}

test "golden: a cluster answer cut short or with a flag out of range is refused" {
    const s = bytesOf("cluster_status.ans");
    try testing.expectError(error.Malformed, wire.Status.decode(s[0 .. s.len - 1]));
    const m = bytesOf("cluster_members.ans");
    try testing.expectError(error.Malformed, wire.Members.decode(m[0 .. m.len - 1]));
    const bad = try testing.allocator.dupe(u8, m);
    defer testing.allocator.free(bad);
    bad[13 + 23] = 3; // the first row's halt flag
    try testing.expectError(error.Malformed, wire.Members.decode(bad));
    try testing.expectError(error.Malformed, wire.Remove.decode(&.{ 9, 0, 0, 0, 2 }));
    var long: [wire.RepairSkip.MAX_REASON + 1]u8 = @splat('x');
    var kbuf: [wire.RepairSkip.MAX_SIZE]u8 = undefined;
    try testing.expectError(error.Malformed, (wire.RepairSkip{ .index = 1, .yes = false, .reason = &long }).encode(&kbuf));
}

test "golden: every hand-coded op has a vector or is listed as having none yet, and the list only shrinks" {
    try testing.expect(NO_VECTOR_YET.len <= NO_VECTOR_YET_CAP);
    inline for (@typeInfo(proto.OpCode).@"enum".field_names) |name| {
        const op = @field(proto.OpCode, name);
        var has_vector = false;
        for (vectors) |v| has_vector = has_vector or v.op == op;
        const listed = std.mem.indexOfScalar(proto.OpCode, &NO_VECTOR_YET, op) != null;
        if (table.isOp(@backingInt(op))) {
            // A table op's layout is in the table hash already.
            if (listed or has_vector) {
                std.debug.print("{s} is on the op table; it needs no vector and no listing\n", .{name});
                return error.TestUnexpectedResult;
            }
        } else if (has_vector == listed) {
            std.debug.print("{s} is coded by hand: it needs a vector, or (never newly) a place in NO_VECTOR_YET; not both\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}
