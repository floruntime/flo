//! A step's own counters, as a run's leader keeps them between steps:
//! logged with each `workflow_step` (`runs_machine.Step.progress`) and
//! read back by whichever leader takes the run's next step, so no decision
//! state is left in one node's memory. The machine stores the bytes and
//! never reads them.
//!
//! `[version:u8 = 3][retry_count:u32][poll_attempt:u32]
//!  [waiting_for_room:u8][call_unregistered_noted:u8][call_over_budget_noted:u8]
//!  [call_over_quota_noted:u8]
//!  [call_retry_at_ms:i64][call_retry_index:u64]
//!  [plan_executor_idx:u8][plan_executor_natural_idx:u8]
//!  [plan_executor_retry_count:u32][plan_order_len:u8][plan_order:u8 * len]`,
//! little-endian. Empty is a run with none taken yet.

const std = @import("std");
const plan_types = @import("plan_types.zig");

pub const VERSION: u8 = 3;

pub const Progress = struct {
    /// Retries taken of the current step.
    retry_count: u32 = 0,
    /// Polls taken of the current step.
    poll_attempt: u32 = 0,
    /// The step's start was refused for room, and waits to be tried again.
    waiting_for_room: bool = false,
    /// The step's call found its action not registered, and said so.
    call_unregistered_noted: bool = false,
    /// The step's call is held by its action's shard's memory share, and
    /// said so.
    call_over_budget_noted: bool = false,
    /// The step's call is held by its callee's namespace's run-history
    /// quota, and said so.
    call_over_quota_noted: bool = false,
    /// When the step's call may next be asked again, and the index of the
    /// last time it was.
    call_retry_at_ms: i64 = 0,
    call_retry_index: u64 = 0,
    /// A plan's pass: where it is in its executor order, the executor's
    /// own index, its retries, and the order itself (`plan_order_len` 0:
    /// not started).
    plan_executor_idx: u8 = 0,
    plan_executor_natural_idx: u8 = 0,
    plan_executor_retry_count: u32 = 0,
    plan_order: [plan_types.MAX_PLAN_EXECUTORS]u8 = @splat(0),
    plan_order_len: u8 = 0,

    pub const max_size = 1 + 4 + 4 + 1 + 1 + 1 + 1 + 8 + 8 + 1 + 1 + 4 + 1 + plan_types.MAX_PLAN_EXECUTORS;

    pub fn encode(p: *const Progress, buf: *[max_size]u8) []const u8 {
        var w: std.Io.Writer = .fixed(buf);
        w.writeByte(VERSION) catch unreachable;
        w.writeInt(u32, p.retry_count, .little) catch unreachable;
        w.writeInt(u32, p.poll_attempt, .little) catch unreachable;
        w.writeByte(@intFromBool(p.waiting_for_room)) catch unreachable;
        w.writeByte(@intFromBool(p.call_unregistered_noted)) catch unreachable;
        w.writeByte(@intFromBool(p.call_over_budget_noted)) catch unreachable;
        w.writeByte(@intFromBool(p.call_over_quota_noted)) catch unreachable;
        w.writeInt(i64, p.call_retry_at_ms, .little) catch unreachable;
        w.writeInt(u64, p.call_retry_index, .little) catch unreachable;
        w.writeByte(p.plan_executor_idx) catch unreachable;
        w.writeByte(p.plan_executor_natural_idx) catch unreachable;
        w.writeInt(u32, p.plan_executor_retry_count, .little) catch unreachable;
        w.writeByte(p.plan_order_len) catch unreachable;
        w.writeAll(p.plan_order[0..p.plan_order_len]) catch unreachable;
        return w.buffered();
    }

    pub const DecodeError = error{ WrongVersion, Malformed };

    /// The counters `bytes` hold; empty for a run with none yet. Bytes of
    /// another version, cut short, or with trailing bytes are refused:
    /// the leader fails the step saying so (`why`).
    pub fn decode(bytes: []const u8) DecodeError!Progress {
        if (bytes.len == 0) return .{};
        if (bytes[0] != VERSION) return error.WrongVersion;
        var r: std.Io.Reader = .fixed(bytes[1..]);
        var p: Progress = .{};
        p.retry_count = r.takeInt(u32, .little) catch return error.Malformed;
        p.poll_attempt = r.takeInt(u32, .little) catch return error.Malformed;
        p.waiting_for_room = (r.takeByte() catch return error.Malformed) != 0;
        p.call_unregistered_noted = (r.takeByte() catch return error.Malformed) != 0;
        p.call_over_budget_noted = (r.takeByte() catch return error.Malformed) != 0;
        p.call_over_quota_noted = (r.takeByte() catch return error.Malformed) != 0;
        p.call_retry_at_ms = r.takeInt(i64, .little) catch return error.Malformed;
        p.call_retry_index = r.takeInt(u64, .little) catch return error.Malformed;
        p.plan_executor_idx = r.takeByte() catch return error.Malformed;
        p.plan_executor_natural_idx = r.takeByte() catch return error.Malformed;
        p.plan_executor_retry_count = r.takeInt(u32, .little) catch return error.Malformed;
        p.plan_order_len = r.takeByte() catch return error.Malformed;
        if (p.plan_order_len > plan_types.MAX_PLAN_EXECUTORS) return error.Malformed;
        const order = r.take(p.plan_order_len) catch return error.Malformed;
        @memcpy(p.plan_order[0..p.plan_order_len], order);
        if (r.seek != r.end) return error.Malformed;
        return p;
    }

    pub fn why(err: DecodeError) []const u8 {
        return switch (err) {
            error.WrongVersion => "the step's logged counters are of another version",
            error.Malformed => "the step's logged counters don't decode",
        };
    }
};

comptime {
    // The machine's bound (`runs_machine.MAX_PROGRESS`) holds every encoding.
    std.debug.assert(Progress.max_size <= 1024);
}

const testing = std.testing;

test "progress: every counter a step decides on round-trips through its logged bytes" {
    var p: Progress = .{
        .retry_count = 3,
        .poll_attempt = 7,
        .waiting_for_room = true,
        .call_unregistered_noted = true,
        .call_over_budget_noted = true,
        .call_over_quota_noted = true,
        .call_retry_at_ms = -5,
        .call_retry_index = 1 << 40,
        .plan_executor_idx = 2,
        .plan_executor_natural_idx = 4,
        .plan_executor_retry_count = 9,
        .plan_order_len = 5,
    };
    for (0..5) |i| p.plan_order[i] = @intCast(4 - i);
    var buf: [Progress.max_size]u8 = undefined;
    const got = try Progress.decode(p.encode(&buf));
    // Field by field, so a field added to the struct and not to the bytes
    // fails here.
    inline for (@typeInfo(Progress).@"struct".field_names) |name| {
        if (comptime std.mem.eql(u8, name, "plan_order")) {
            try testing.expectEqualSlices(u8, p.plan_order[0..p.plan_order_len], got.plan_order[0..got.plan_order_len]);
        } else try testing.expectEqual(@field(p, name), @field(got, name));
    }
    // None yet: zero counters.
    try testing.expectEqual(@as(u32, 0), (try Progress.decode("")).retry_count);
}

test "progress: another version, a cut, or trailing bytes are refused, never read" {
    const p: Progress = .{ .retry_count = 1, .plan_order_len = 2 };
    var buf: [Progress.max_size]u8 = undefined;
    const bytes = p.encode(&buf);
    var other = buf;
    other[0] = VERSION + 1;
    try testing.expectError(error.WrongVersion, Progress.decode(other[0..bytes.len]));
    for (1..bytes.len) |n| try testing.expectError(error.Malformed, Progress.decode(bytes[0..n]));
    var long: [Progress.max_size + 1]u8 = undefined;
    @memcpy(long[0..bytes.len], bytes);
    long[bytes.len] = 0;
    try testing.expectError(error.Malformed, Progress.decode(long[0 .. bytes.len + 1]));
    // An order longer than a plan's executors can be.
    var bad = buf;
    bad[bytes.len - 3] = plan_types.MAX_PLAN_EXECUTORS + 1;
    try testing.expectError(error.Malformed, Progress.decode(bad[0..bytes.len]));
}
