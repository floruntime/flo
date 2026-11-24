//! The workflow family's ops on the table. The namespace and key travel in
//! the envelope; the key is the run id, whose bits name the partition the
//! run lives on.

const ops = @import("ops.zig");
const schema = @import("schema.zig");
const history = @import("../stream/history_read.zig");

pub const table = [_]ops.Op{
    .{
        .name = "workflow.history",
        .code = 0x345,
        .class = .read,
        .route = .run_id,
        .request = history.Request,
        .response = history.Response,
        // By hand: it follows the cursor to `--limit` events.
        .cli = .{ .expose = .hand, .key = .{ .name = "run_id", .doc = "The run's id" }, .render = .table, .examples = &.{"flo workflow history wf-1a2b3c4d5e6f00-3-1c"} },
        .doc = "A page of a workflow run's history: what happened to it, in order.",
    },
    .{
        .name = "workflow.run_delete",
        .code = 0x34B,
        .class = .write,
        .route = .run_id,
        .request = schema.Record(&.{}),
        .response = schema.Record(&.{}),
        .cli = .{ .key = .{ .name = "run_id", .doc = "The run's id" }, .examples = &.{"flo workflow run-delete wf-1a2b3c4d5e6f00-3-1c"} },
        .doc = "Delete an ended workflow run and its history; a live run is refused (cancel it first). Its id and idempotency key may then start a new run.",
    },
};
