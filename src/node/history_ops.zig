//! `workflow.history` and `action.history`: a run's history, for either
//! producer, read from its row and the history families of its own group.
//! A read whose chunks haven't all landed, or are being read back from
//! disk, is held until they do, or until `history_read.HOLD_MS`.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const he = @import("../protocol/history_entries.zig");
const shard_mod = @import("shard.zig");
const connection_mod = @import("connection.zig");
const table_run = @import("table_run.zig");
const waiter_pool_mod = @import("waiter_pool.zig");
const view_mod = @import("../contract/view.zig");
const history = @import("../stream/history_read.zig");
const runs_machine = @import("../workflow/runs_machine.zig");
const action_machine = @import("../actions/machine.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const Request = proto.Request;
const Waiter = waiter_pool_mod.Waiter;
const Response = history.Response;

/// What a history op answers its family's errors with.
pub const Ops = struct {
    pub const Error = error{ NotFound, OutOfMemory, DiskLate, WaitersFull };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.NotFound => .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = "not found: no such run" } },
            error.OutOfMemory => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } },
            error.DiskLate => .{ .status = .unavailable, .refusal = .{ .reason = .unclassified, .ran = .no, .message = shard_mod.DISK_READ_LATE } },
            error.WaitersFull => .{ .status = .overloaded, .refusal = .{ .reason = .too_many_waiters, .ran = .no, .message = "overloaded: too many reads waiting on this shard — retry" } },
        };
    }

    pub fn workflowHistory(shard: *Shard, conn: *Connection, req: Request, limit: u32, cursor: ?u64, newest_first: bool) Error!table_run.Outcome(Response) {
        return asked(shard, conn, req, .workflow, .{ .limit = limit, .cursor = cursor, .newest_first = newest_first });
    }

    pub fn actionHistory(shard: *Shard, conn: *Connection, req: Request, limit: u32, cursor: ?u64, newest_first: bool) Error!table_run.Outcome(Response) {
        return asked(shard, conn, req, .action, .{ .limit = limit, .cursor = cursor, .newest_first = newest_first });
    }
};

/// The most of a read's scratch kept for the next: a page or two of small
/// events; a larger read's is freed as the next begins.
pub const SCRATCH_KEPT: usize = 256 << 10;

/// The answer's events live in the shard's history scratch until the next
/// read, which is after the table encodes this one.
fn asked(shard: *Shard, conn: *Connection, req: Request, family: he.Family, q: history.Query) Ops.Error!table_run.Outcome(Response) {
    // The namespace is the request's, as every op's: its run is read in
    // that namespace only.
    var kb: [history.KEY_LEN]u8 = undefined;
    const key = history.waiterKey(&kb, family, req.ns_id, req.key) catch return error.NotFound;
    // What a large read took isn't kept past the next: the budget holds
    // only `SCRATCH_KEPT`.
    const scratch = &shard.history_scratch;
    _ = scratch.reset(.{ .retain_with_limit = SCRATCH_KEPT });
    switch (try readOf(shard, scratch.allocator(), family, key[1..], q, false)) {
        .page => |p| return .{ .answer = try history.response(scratch.allocator(), p) },
        .disk_late => return error.DiskLate,
        .wait => {
            const registered = shard.waiter_pool.register(.{
                .kind = .history_read,
                .reply_to = conn.replyTo(),
                .request_id = req.header.request_id,
                .key = key,
                .min_version = q.cursor orelse 0,
                .stream = .{ .limit = q.limit, .partition = q.flags() },
                .timeout_ms = history.HOLD_MS,
            });
            if (!registered) return error.WaitersFull;
            return .wait;
        },
    }
}

/// Run `run_key` (its namespace, then its id) of `family`'s history, as its
/// row and its group's history families hold it.
fn readOf(shard: *Shard, a: std.mem.Allocator, family: he.Family, run_key: []const u8, q: history.Query, final: bool) Ops.Error!history.Answer {
    const stream = shard.stream_handler;
    const rv: view_mod.ReadView = shard.workflow_handler.defsRead();
    const ns: NamespaceId = .of(std.mem.readInt(u32, run_key[0..4], .big));
    const run_id = run_key[4..];
    const src: history.Source = switch (family) {
        .workflow => blk: {
            const rec = runs_machine.runOf(rv, run_key) orelse return error.NotFound;
            const ended = (std.enums.fromInt(runs_machine.RunStatus, rec.status) orelse runs_machine.RunStatus.failed).isTerminal();
            break :blk .{ .family = .workflow, .ns = ns, .owner = rec.workflow, .run = run_id, .created_ns = rec.created_ns, .ended_ms = if (ended) @intCast(@max(rec.completed_at_ms, 0)) else null, .flushed = rec.hist_flushed, .pending = rec.hist_pending };
        },
        .action => blk: {
            const got = rv.get(action_machine.Runs, run_key) catch return error.NotFound;
            const rec = (got orelse return error.NotFound).v;
            break :blk .{ .family = .action, .ns = ns, .owner = rec.action, .run = run_id, .created_ns = rec.created_ns, .ended_ms = if (rec.completed_ns != 0) rec.completed_ns / std.time.ns_per_ms else null, .flushed = rec.hist_flushed, .pending = rec.hist_pending };
        },
    };
    return history.read(a, stream, rv, src, q.window(), q.limit, final);
}

/// A held history read, asked again: its chunks landed, or came back from
/// disk, or (`final`) its deadline came. True once answered.
pub fn resolve(shard: *Shard, waiter: *const Waiter, final: bool) bool {
    const of = history.ofWaiterKey(waiter.key()) orelse {
        shard.deliverDeferredRefusal(waiter.reply_to, waiter.request_id, .not_found, proto.notRun(""));
        return true;
    };
    const q = history.Query.fromHeld(waiter.stream.limit, waiter.stream.partition orelse 0, waiter.min_version);
    var arena = std.heap.ArenaAllocator.init(shard.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const refuse = struct {
        fn with(s: *Shard, w: *const Waiter, err: Ops.Error) bool {
            const r = Ops.refusal(err);
            s.deliverDeferredRefusal(w.reply_to, w.request_id, r.status, r.refusal);
            return true;
        }
    }.with;
    const answer = readOf(shard, a, of.family, of.run_key, q, final) catch |err| return refuse(shard, waiter, err);
    // A run gone since the read was held is answered as missing: not found.
    switch (answer) {
        .wait => return false,
        .disk_late => return refuse(shard, waiter, error.DiskLate),
        .page => |p| {
            const v = history.response(a, p) catch return refuse(shard, waiter, error.OutOfMemory);
            const buf = a.alloc(u8, Response.encodedSize(v) catch return refuse(shard, waiter, error.OutOfMemory)) catch return refuse(shard, waiter, error.OutOfMemory);
            const bytes = Response.encodeInto(buf, v) catch return refuse(shard, waiter, error.OutOfMemory);
            shard.deliverDeferredOk(waiter.reply_to, waiter.request_id, bytes);
            return true;
        },
    }
}
