//! The worker family's ops on the table. The namespace travels in the
//! envelope, so it isn't a field.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;
const Defaulted = schema.Defaulted;

/// What kind of work a worker registered for.
pub const WorkerType = enum(u8) {
    action = 0,
    stream = 1,
};

/// A worker's state, as its record holds it.
pub const WorkerStatus = enum(u8) {
    /// Heartbeating, and taking work.
    active = 0,
    idle = 1,
    /// Finishing its work, taking none.
    draining = 2,
    /// It stopped heartbeating.
    unhealthy = 3,
};

/// The most workers one page lists.
pub const MAX_LIST: u32 = 1024;

/// The longest machine id a worker registers with: a host name.
pub const MAX_MACHINE_ID: u32 = 255;

/// A worker id: a key in the registry.
pub const WorkerId = Bytes(ops.MAX_KEY);

pub const WorkerRow = Record(&.{
    .{ .name = "id", .T = WorkerId },
    .{ .name = "worker_type", .T = schema.Enum(WorkerType) },
    .{ .name = "status", .T = schema.Enum(WorkerStatus) },
    .{ .name = "current_load", .T = Int(u32) },
    .{ .name = "max_concurrency", .T = Int(u32) },
    .{ .name = "tasks_completed", .T = Int(u64) },
    .{ .name = "tasks_failed", .T = Int(u64) },
    .{ .name = "registered_at_ms", .T = Int(i64) },
    .{ .name = "last_heartbeat_ms", .T = Int(i64) },
    .{ .name = "machine_id", .T = Optional(Bytes(MAX_MACHINE_ID)) },
});

/// The cursor field's largest encoding: presence, then an id.
const cursor_size = 1 + WorkerId.max_size;

/// What a process on a worker runs.
pub const ProcessKind = enum(u8) {
    action = 0,
    stream_consumer = 1,
};

/// The most processes a worker registers.
pub const MAX_PROCESSES: u32 = 1024;

/// The longest metadata a worker registers with.
pub const MAX_METADATA: u32 = std.math.maxInt(u16);

pub const ProcessRow = Record(&.{
    .{ .name = "name", .T = Bytes(ops.MAX_KEY) },
    .{ .name = "kind", .T = schema.Enum(ProcessKind) },
    .{ .name = "run_count", .T = Int(u64) },
    .{ .name = "fail_count", .T = Int(u64) },
    .{ .name = "last_run_at_ms", .T = Int(i64) },
});

const Metadata = Optional(Bytes(MAX_METADATA));

/// The processes list's count prefix: an empty list of them is just that.
const processes_count_size = schema.BoundedList(ProcessRow, MAX_PROCESSES, ProcessRow.max_size).min_size;

/// A worker's processes, as one answer holds them: whatever the row, the
/// largest metadata and the list's count prefix leave of it. Registration
/// refuses processes that wouldn't fit.
pub const Processes = schema.BoundedList(ProcessRow, MAX_PROCESSES, @intCast(ops.MAX_RESPONSE_BODY - processes_count_size - WorkerRow.max_size - Metadata.max_size));

pub const table = [_]ops.Op{
    .{
        .name = "worker.list",
        .code = 0x323,
        .class = .read,
        // Every shard holds the workers that registered on it.
        .fanout = .walk,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_LIST), MAX_LIST), .doc = "Most workers on the page" },
            .{ .name = "cursor", .T = WorkerId, .doc = "The last id of the page before; omit to start" },
            .{ .name = "action", .T = Bytes(ops.MAX_KEY), .doc = "Only workers that run this action; omit for all" },
        }),
        .response = Record(&.{
            .{ .name = "workers", .T = schema.BoundedList(WorkerRow, MAX_LIST, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - cursor_size)), .doc = "The namespace's workers, in id order" },
            // The last id on the page; null on the last page.
            .{ .name = "cursor", .T = Optional(WorkerId), .doc = "Where the next page starts; none on the last page" },
        }),
        // It follows the cursor to the last page, so it stays written by hand.
        .cli = .{ .expose = .hand, .key = null, .render = .table, .examples = &.{"flo worker list"} },
        .doc = "List a namespace's workers in id order, a page at a time.",
    },
    .{
        .name = "worker.info",
        .code = 0x324,
        .class = .read,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "worker", .T = WorkerRow, .doc = "The worker, as worker.list shows it" },
            .{ .name = "metadata", .T = Metadata, .doc = "What the worker registered with, as given" },
            .{ .name = "processes", .T = Processes, .doc = "What the worker runs, as registered, with its counts" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "worker_id", .doc = "The worker's id" }, .render = .record, .examples = &.{"flo worker info w1"} },
        .doc = "A worker's record: its row, its metadata and its processes.",
    },
};

/// The worker op in the table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no worker op named '" ++ name ++ "'");
}

comptime {
    ops.check(&table);
}

test "worker.info's largest answer is exactly one response body" {
    try std.testing.expectEqual(ops.MAX_RESPONSE_BODY, op("worker.info").response.max_size);
}
