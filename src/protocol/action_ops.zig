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
const Defaulted = schema.Defaulted;
const history = @import("../stream/history_read.zig");

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
/// The longest worker id: a worker's id is its request's key.
pub const MAX_WORKER_ID: u32 = ops.MAX_KEY;

/// The longest action name.
pub const MAX_ACTION_NAME: u32 = 256;
pub const ActionName = Bytes(MAX_ACTION_NAME);
pub const WorkerId = Bytes(MAX_WORKER_ID);
/// The most actions one await names.
pub const MAX_AWAIT_ACTIONS: u32 = 64;
/// The longest a worker's await waits for a task.
pub const MAX_AWAIT_MS: u64 = 300_000;
/// A named outcome a workflow step follows: "success" when none is named.
pub const MAX_OUTCOME: u32 = 256;
pub const MAX_HELD_NOTE: u32 = 128;
/// The longest input a task carries: an invoke is one log entry.
pub const MAX_INPUT: u32 = 64 * 1024;
/// A retried await's key: the same key gets the same task while its lease
/// runs.
pub const MAX_REQUEST_KEY: u32 = 64;
/// The longest a failed run waits before it's ready again.
pub const MAX_DELAY_MS: u64 = 24 * 60 * 60 * 1000;

/// A task: a delivery of a run to one worker. `run`, `deliveries`, `term`
/// and `index` name the delivery, and a complete, fail or touch names it
/// back; a report naming any other delivery of the run is refused stale.
pub const Task = Record(&.{
    .{ .name = "run", .T = Bytes(ops.MAX_KEY), .doc = "The run's id" },
    .{ .name = "namespace", .T = Bytes(@import("limits.zig").MAX_NAMESPACE_NAME), .doc = "The run's namespace" },
    .{ .name = "deliveries", .T = Int(u32), .doc = "The run's deliveries, this one included" },
    .{ .name = "term", .T = Int(u64), .doc = "The term the delivery was made in" },
    .{ .name = "index", .T = Int(u64), .doc = "The log index the delivery was made at" },
    .{ .name = "action", .T = ActionName, .doc = "The action the run invokes" },
    .{ .name = "created_at_ms", .T = Int(i64), .doc = "When the run was invoked, in Unix milliseconds" },
    .{ .name = "attempt", .T = Int(u32), .doc = "The run's failures so far, plus one" },
    .{ .name = "lease_ms", .T = Int(u64), .doc = "How long the worker holds it without a touch" },
    .{ .name = "caller_run", .T = Optional(Bytes(ops.MAX_KEY)), .doc = "The workflow run that invoked it, if one did" },
    .{ .name = "caller_workflow", .T = Optional(Bytes(ops.MAX_KEY)), .doc = "That run's workflow" },
    .{ .name = "input", .T = Bytes(MAX_INPUT), .doc = "The run's input, as invoked" },
});

/// A delivery a worker reports on: the task's own fields, and the action,
/// which the report must name to reach the run at all.
const delivery_fields = [_]schema.Field{
    .{ .name = "action", .T = ActionName, .doc = "The task's action" },
    .{ .name = "worker", .T = WorkerId, .doc = "The worker reporting: the one the task was given to, or the report is refused" },
    .{ .name = "deliveries", .T = Int(u32), .doc = "The task's deliveries" },
    .{ .name = "term", .T = Int(u64), .doc = "The task's term" },
    .{ .name = "index", .T = Int(u64), .doc = "The task's index" },
};

/// Where an action's run list stands: newest first, then by id.
pub const RunCursor = Record(&.{
    .{ .name = "created_at_ms", .T = Int(i64), .doc = "The run's invoke time, in Unix milliseconds" },
    .{ .name = "run", .T = Bytes(@import("action_entries.zig").MAX_RUN_ID), .doc = "The run's id" },
});

/// One run as an action's list shows it.
pub const ListedRun = Record(&.{
    .{ .name = "run", .T = Bytes(@import("action_entries.zig").MAX_RUN_ID), .doc = "The run's id" },
    .{ .name = "status", .T = schema.Enum(RunStatus), .doc = "Where the run is" },
    .{ .name = "created_at_ms", .T = Int(i64), .doc = "When it was invoked, in Unix milliseconds" },
    .{ .name = "started_at_ms", .T = Optional(Int(i64)), .doc = "When a worker first took it" },
    .{ .name = "completed_at_ms", .T = Optional(Int(i64)), .doc = "When it ended" },
});

/// The most runs one page of an action's list holds.
pub const MAX_LIST_RUNS: u32 = 1024;
pub const ListedRuns = schema.BoundedList(ListedRun, MAX_LIST_RUNS, MAX_LIST_RUNS * ListedRun.max_size);

pub const table = [_]ops.Op{
    .{
        .name = "action.list_runs",
        .code = 0x304,
        .class = .read,
        .route = .key,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_LIST_RUNS), 100), .doc = "Most runs to show" },
            .{ .name = "after", .T = Optional(RunCursor), .doc = "Go on past this run, as the last answer's cursor named it" },
        }),
        .response = Record(&.{
            .{ .name = "runs", .T = ListedRuns, .doc = "The action's runs, newest first; may be none while more follow" },
            .{ .name = "cursor", .T = Optional(RunCursor), .doc = "Where the next page starts; none on the last page" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "action", .doc = "The action's name" } },
        .doc = "An action's runs, newest first, a page at a time: each run's id, status and times.",
    },
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
            .{ .name = "output", .T = Optional(Bytes(MAX_OUTPUT)), .doc = "What a completed run returned: its first 256 bytes" },
            // Why its last attempt failed.
            .{ .name = "error_message", .T = Optional(Bytes(MAX_ERROR)), .doc = "The error its last attempt failed with: its first 256 bytes" },
            .{ .name = "worker_id", .T = Optional(Bytes(MAX_WORKER_ID)), .doc = "The worker that holds the run, or last held it" },
            .{ .name = "outcome", .T = Optional(Bytes(MAX_OUTCOME)), .doc = "The outcome a completed run's worker named, which a workflow step follows; none for success" },
            .{ .name = "held", .T = Optional(Bytes(MAX_HELD_NOTE)), .doc = "Why its lease, past due, isn't ended: its end waits for room on the calling workflow's shard" },
            // At the end, so a decoder that stops before them still reads the rest.
            .{ .name = "output_truncated", .T = schema.Bool, .doc = "Whether output is only the result's first 256 bytes; the whole is the run's action_settled history event" },
            .{ .name = "error_truncated", .T = schema.Bool, .doc = "Whether error_message is only the error's first 256 bytes; the whole is in the run's history" },
        }),
        .cli = .{ .expose = .hand, .render = .record, .examples = &.{"flo action status act-1a2b3c4d5e6f00-3-1c"} },
        .doc = "A run's status, timestamps, attempts, worker, and its output or last error.",
    },
    .{
        .name = "action.history",
        .code = 0x310,
        .class = .read,
        .route = .run_id,
        .request = history.Request,
        .response = history.Response,
        .cli = .{ .key = .{ .name = "run_id", .doc = "The run's id" }, .render = .table, .examples = &.{"flo action history act-1a2b3c4d5e6f00-3-1c"} },
        .doc = "A page of an action run's history: its invoke, deliveries, failures and result, in order.",
    },
    .{
        .name = "action.run_delete",
        .code = 0x311,
        .class = .write,
        .route = .run_id,
        .request = Record(&.{}),
        .response = Record(&.{}),
        .cli = .{ .key = .{ .name = "run_id", .doc = "The run's id" }, .examples = &.{"flo action run-delete act-1a2b3c4d5e6f00-3-1c"} },
        .doc = "Delete an ended action run and its history; a live run is refused (cancel it first). Its id may then name a new run.",
    },
    .{
        .name = "action.await",
        .code = 0x30A,
        .class = .write,
        .takes_items = true,
        .wait = .{ .field = "block_ms", .default_ms = 30_000, .max_ms = MAX_AWAIT_MS },
        // On the worker's shard, where its registration is: its labels,
        // whether it drains, and the actions it runs.
        .route = .key,
        .request = Record(&.{
            .{ .name = "actions", .T = schema.BoundedList(ActionName, MAX_AWAIT_ACTIONS, MAX_AWAIT_ACTIONS * ActionName.max_size), .doc = "The actions to take a task of, the first tried first" },
            .{ .name = "block_ms", .T = Defaulted(schema.DurationMs(MAX_AWAIT_MS), 30_000), .doc = "How long to wait for a task; 0 answers at once" },
            .{ .name = "request_key", .T = Optional(Bytes(MAX_REQUEST_KEY)), .doc = "Retrying with the same key gets the task the first try got" },
        }),
        .response = Record(&.{
            .{ .name = "task", .T = Optional(Task), .doc = "The task, or none if no run was ready in time" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "worker_id", .doc = "The worker's id" } },
        .doc = "Wait for a task of one of the worker's actions, and take it.",
    },
    .{
        .name = "action.complete",
        .code = 0x30B,
        .class = .write,
        .route = .run_id,
        .request = Record(&(delivery_fields ++ [_]schema.Field{
            .{ .name = "outcome", .T = Defaulted(Bytes(MAX_OUTCOME), ""), .doc = "The outcome a workflow step follows; none is \"success\"" },
            .{ .name = "result", .T = Defaulted(Bytes(MAX_OUTPUT), ""), .doc = "What the run returns" },
        })),
        .response = Record(&.{}),
        .cli = .{ .expose = .hand, .key = .{ .name = "task_id", .doc = "The task's id, as await printed it" } },
        .doc = "Complete a task: its run is done, with this result.",
    },
    .{
        .name = "action.fail",
        .code = 0x30C,
        .class = .write,
        .route = .run_id,
        .request = Record(&(delivery_fields ++ [_]schema.Field{
            .{ .name = "retry", .T = Defaulted(schema.Bool, true), .doc = "Run it again while attempts are left; false fails the run now" },
            .{ .name = "abandoned", .T = Defaulted(schema.Bool, false), .doc = "Hand it back untried: counts no failure" },
            .{ .name = "delay_ms", .T = Defaulted(schema.DurationMs(MAX_DELAY_MS), 0), .doc = "How long before it is ready again" },
            .{ .name = "error", .T = Defaulted(Bytes(MAX_ERROR), ""), .doc = "Why it failed" },
        })),
        .response = Record(&.{}),
        .cli = .{ .expose = .hand, .key = .{ .name = "task_id", .doc = "The task's id, as await printed it" } },
        .doc = "Fail a task: its run is ready again, unless its attempts are spent or retry is false.",
    },
    .{
        .name = "action.touch",
        .code = 0x30D,
        .class = .write,
        .route = .run_id,
        .request = Record(&delivery_fields),
        .response = Record(&.{
            .{ .name = "expires_at_ms", .T = Int(i64), .doc = "When the lease runs out without another touch, in Unix milliseconds" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "task_id", .doc = "The task's id, as await printed it" } },
        .doc = "Keep a task: with half its lease or less left, its lease runs a full lease from now, up to 12 h from its delivery; with more left, nothing changes. Touch at least every half lease.",
    },
    .{
        .name = "action.deliver",
        .code = 0x30E,
        .class = .write,
        .origin = .internal,
        .takes_items = true,
        // On the action's shard, as an invoke is.
        .route = .key,
        .request = Record(&.{
            .{ .name = "holder", .T = WorkerId, .doc = "The worker that takes it" },
            .{ .name = "labels", .T = Optional(Bytes(std.math.maxInt(u16))), .doc = "The worker's labels, for runs that require some" },
            .{ .name = "request_key", .T = Optional(Bytes(MAX_REQUEST_KEY)), .doc = "The await's request key" },
            .{ .name = "again_only", .T = Defaulted(schema.Bool, false), .doc = "Only the task the request key got before, if still held: a retry looks there first" },
        }),
        .response = Record(&.{
            .{ .name = "task", .T = Optional(Task), .doc = "The task, or none if no run is ready" },
        }),
        .cli = .{ .expose = .none, .key = .{ .name = "action" } },
        .doc = "A worker's shard asking an action's shard for a task of it.",
    },
    .{
        .name = "action.info",
        .code = 0x30F,
        .class = .read,
        // On the action's shard, where its definition and runs are.
        .route = .key,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "owner", .T = Bytes(MAX_OWNER), .doc = "Who registered it, as given" },
            .{ .name = "version", .T = Int(u32), .doc = "Its version, counting registrations" },
            .{ .name = "timeout_ms", .T = Int(u64), .doc = "How long a worker holds a run before it times out" },
            .{ .name = "max_attempts", .T = Int(u32), .doc = "Attempts a run gets before it fails" },
            .{ .name = "created_at_ms", .T = Int(i64), .doc = "When it was first registered, in Unix milliseconds" },
            // Its runs by status, kept as they move, so exact.
            .{ .name = "ready", .T = Int(u64), .doc = "Its runs waiting for a worker" },
            .{ .name = "leased", .T = Int(u64), .doc = "Its runs a worker holds" },
            .{ .name = "completed", .T = Int(u64), .doc = "Its runs that completed" },
            .{ .name = "failed", .T = Int(u64), .doc = "Its runs that failed for good" },
            .{ .name = "timed_out", .T = Int(u64), .doc = "Its runs whose last attempt timed out" },
            // How long its completed runs took, from a worker first taking
            // one to its completion; none until one completes. Sampled from
            // its newest runs, a bounded read per ask.
            .{ .name = "latency_sampled", .T = schema.Bool, .doc = "The latencies are from the newest 10,000 runs, not all of them" },
            .{ .name = "latency_count", .T = Optional(Int(u64)), .doc = "Completed runs measured" },
            .{ .name = "latency_avg_ms", .T = Optional(Int(u64)), .doc = "Their mean duration" },
            .{ .name = "latency_p50_ms", .T = Optional(Int(u64)), .doc = "Their median duration" },
            .{ .name = "latency_p95_ms", .T = Optional(Int(u64)), .doc = "Their 95th percentile duration" },
            .{ .name = "latency_max_ms", .T = Optional(Int(u64)), .doc = "The longest" },
        }),
        .cli = .{ .expose = .command, .key = .{ .name = "action", .doc = "The action's name" }, .render = .record, .examples = &.{"flo action info send-email"} },
        .doc = "An action's definition, its runs by status and how long its newest completed runs took.",
    },
};

/// The most runs `action.info` reads for its latency sample, its newest,
/// so one answer is bounded however many runs an action keeps.
pub const RUNS_SCANNED: u32 = 10_000;

/// The longest owner an action registers with.
pub const MAX_OWNER: u32 = 256;

pub const ActionNames = schema.BoundedList(ActionName, MAX_AWAIT_ACTIONS, MAX_AWAIT_ACTIONS * ActionName.max_size);

/// `names` as an await's `actions`, encoded into `buf`.
pub fn actionNames(buf: []u8, names: []const []const u8) schema.EncodeError!ActionNames.Value {
    if (names.len > MAX_AWAIT_ACTIONS) return error.TooLong;
    var w: schema.Writer = .{ .buf = buf };
    for (names) |n| try ActionName.encode(&w, n);
    return .{ .bytes = buf[0..w.pos], .count = @intCast(names.len) };
}

/// A task's id as a person copies it: `run:deliveries:term:index:action`.
/// A run id holds no colon, and the action comes last, so it may.
pub const TaskId = struct {
    run: []const u8,
    deliveries: u32,
    term: u64,
    index: u64,
    action: []const u8,

    pub fn of(t: Task.Value) TaskId {
        return .{ .run = t.run, .deliveries = t.deliveries, .term = t.term, .index = t.index, .action = t.action };
    }

    pub fn format(self: TaskId, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}:{d}:{d}:{d}:{s}", .{ self.run, self.deliveries, self.term, self.index, self.action });
    }

    pub fn parse(s: []const u8) ?TaskId {
        var it = std.mem.splitScalar(u8, s, ':');
        const run = it.next() orelse return null;
        const deliveries = std.fmt.parseInt(u32, it.next() orelse return null, 10) catch return null;
        const term = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
        const index = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
        const action = it.rest();
        if (run.len == 0 or action.len == 0) return null;
        return .{ .run = run, .deliveries = deliveries, .term = term, .index = index, .action = action };
    }
};

test "a task id round-trips, its action keeping its colons, and a cut one doesn't parse" {
    var buf: [128]u8 = undefined;
    const id: TaskId = .{ .run = "act-1", .deliveries = 2, .term = 3, .index = 40, .action = "send:mail" };
    const text = try id.format(&buf);
    try std.testing.expectEqualStrings("act-1:2:3:40:send:mail", text);
    const back = TaskId.parse(text).?;
    try std.testing.expectEqualStrings("send:mail", back.action);
    try std.testing.expectEqual(@as(u64, 40), back.index);
    for ([_][]const u8{ "", "act-1", "act-1:2:3:40", "act-1:2:3:40:", ":2:3:40:a", "act-1:x:3:40:a" }) |cut| try std.testing.expect(TaskId.parse(cut) == null);
}

/// The action op in the table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no action op named '" ++ name ++ "'");
}
