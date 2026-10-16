//! The action family's ops on the table. The namespace and key travel in
//! the envelope, so they aren't fields; for `action.status` the key is the
//! run id, whose bits name the partition the run lives on.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;

/// Where a run is: the values `action.status` answers with.
pub const RunStatus = enum(u8) {
    /// Waiting for a worker to claim it.
    pending = 0,
    /// A worker holds it.
    running = 1,
    completed = 2,
    /// Failed for good: without retry, or on its last attempt.
    failed = 3,
    cancelled = 4,
    /// Its last attempt ran past its lease without a report.
    timed_out = 5,
};

/// The longest output a run answers with: a complete carries its result
/// with a u16 length.
pub const MAX_OUTPUT: u32 = 65_535;

/// The longest error message a run answers with: a run stores its error
/// with a u16 length.
pub const MAX_ERROR: u32 = 65_535;

pub const table = [_]ops.Op{
    .{
        .name = "action.status",
        .code = 0x302,
        .class = .read,
        .route = .run_id,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "status", .T = schema.Enum(RunStatus), .doc = "Where the run is: pending, running, completed, failed, cancelled or timed_out" },
            .{ .name = "created_at_ms", .T = Int(i64), .doc = "When it was invoked, in Unix milliseconds" },
            .{ .name = "started_at_ms", .T = Optional(Int(i64)), .doc = "When a worker first took it" },
            .{ .name = "completed_at_ms", .T = Optional(Int(i64)), .doc = "When it finished" },
            // Attempts begun so far, the current one included.
            .{ .name = "attempt", .T = Int(u32), .doc = "Attempts so far, counting from 1 once a worker has taken it" },
            // What the worker completed it with.
            .{ .name = "output", .T = Optional(Bytes(MAX_OUTPUT)), .doc = "What a completed run returned" },
            // Why its last attempt failed.
            .{ .name = "error_message", .T = Optional(Bytes(MAX_ERROR)), .doc = "The error its last attempt failed with" },
        }),
        .cli = .{ .expose = .hand, .render = .record, .examples = &.{"flo action status act-1a2b3c4d5e6f00-3"} },
        .doc = "A run's status, timestamps, attempts, and its output or last error.",
    },
};

/// The action op in the table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no action op named '" ++ name ++ "'");
}
