//! Actions on the contract: definitions and runs are four families,
//! changed only through a `View` in `apply`, so a refused entry changes
//! nothing and every replica holds the same runs.
//!
//! - `action.def`: each action's definition, keyed (ns, name).
//! - `action.run`: each run, keyed (ns, run id).
//! - `action.ready`: each ready run, keyed (ns, action, due, run), so a
//!   leader picks the next run of an action without a scan.
//! - `action.deadline`: each lease's deadline, keyed (expiry, ns, run) in
//!   no namespace, so the expire duty reads the earliest without a scan.
//!
//! The namespace is its id, leading each key (inside the deadline key), so
//! one action or run id in two namespaces is two keys.
//!
//! The ready, deadline and request families and the lease rules are the
//! delivery model's (`contract/delivery.zig`).
//!
//! A lease's identity is its deadline key: a run leased until exactly that
//! instant is the lease the key names. Deliver and extend each write one.
//! Settle, release and extend name the expiry the leader saw and delete
//! that key when it is still the run's. One that misses (an extend landed
//! in between) leaves its key to the expire duty, which names every
//! past-due key it reads: the expire deletes each, and ends the lease
//! whose key it is.
//!
//! The bound: an extend must name the delivery's own lease and is a no-op
//! while more than half of it remains, so one lease's keys are written at
//! least half a lease apart, and each is past due one lease after it was
//! written. With the duty dropping past-due keys, a lease holds at most
//! three. No delivery is held past `MAX_HOLD_MS` from its deliver, however
//! often it's touched.
//!
//! Every report names its scope, namespace and action, which apply checks
//! first: a mismatch answers as a missing run, so the token is a
//! staleness fence, not the boundary between tenants.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const client_names = @import("../protocol/names.zig");
const ae = @import("../protocol/action_entries.zig");
const action_ops = @import("../protocol/action_ops.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const hq = @import("../budget/history_quota.zig");
const store_mod_top = @import("../contract/store.zig");
const history_reserve = @import("../budget/history_reserve.zig");
const gate = @import("../budget/gate.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const calls = @import("calls_machine.zig");
const hp = @import("../stream/history_producer.zig");
const he = @import("../protocol/history_entries.zig");
const channels = @import("../contract/channels.zig");
const router = @import("../node/router.zig");
const fnv1a64 = @import("stdx").hash.fnv1a64;
const plant = @import("../plant.zig");
const settings = @import("../settings/machine.zig");
const shareHeld = @import("../raft/node.zig").shareHeld;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;

const Stored = delivery.Stored;
const NamespaceId = @import("../namespace/table.zig").Id;

pub const Def = Record(&.{
    .{ .name = "owner", .T = Bytes(ae.MAX_OWNER) },
    .{ .name = "version", .T = Int(u32) },
    .{ .name = "lease_ms", .T = Int(u64) },
    .{ .name = "max_attempts", .T = Int(u32) },
    .{ .name = "created_ns", .T = Int(u64) },
});

pub const Status = enum(u8) { ready = 0, leased = 1, completed = 2, failed = 3, timed_out = 4 };

pub const Ended = delivery.Ended;

pub const Run = Record(&.{
    .{ .name = "action", .T = ae.ActionName },
    .{ .name = "status", .T = schema.Enum(Status) },
    .{ .name = "input", .T = Bytes(ae.MAX_INPUT) },
    .{ .name = "labels", .T = Optional(Bytes(ae.MAX_LABELS)) },
    .{ .name = "source", .T = schema.Enum(ae.Source) },
    .{ .name = "caller_run", .T = Optional(Bytes(limits.MAX_QUALIFIED_KEY)) },
    .{ .name = "caller_workflow", .T = Optional(Bytes(limits.MAX_QUALIFIED_KEY)) },
    .{ .name = "created_ns", .T = Int(u64) },
    // Its ready key's due time, while ready.
    .{ .name = "due_ns", .T = Int(u64) },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "failures", .T = Int(u32) },
    .{ .name = "max_attempts", .T = Int(u32) },
    // The latest delivery: its holder, token, lease, when it began and
    // until when it runs.
    .{ .name = "holder", .T = ae.Holder },
    .{ .name = "request_key", .T = ae.RequestKey },
    .{ .name = "token", .T = ae.Token },
    .{ .name = "lease_ms", .T = Int(u64) },
    .{ .name = "delivered_ns", .T = Int(u64) },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "ended", .T = schema.Enum(Ended) },
    .{ .name = "started_ns", .T = Int(u64) },
    .{ .name = "completed_ns", .T = Int(u64) },
    // A completed run's named outcome, which a workflow step follows: a
    // worker's "failure" completes the run and takes the failure branch.
    .{ .name = "outcome", .T = ae.Outcome },
    // Its first `PREVIEW` bytes, and whether there were more: the whole is
    // in its history. A result only once completed, with the whole's hash,
    // which a settle's resend is held to; an error once an attempt failed.
    .{ .name = "result", .T = Bytes(PREVIEW) },
    .{ .name = "result_truncated", .T = schema.Bool },
    .{ .name = "result_hash", .T = Int(u64) },
    .{ .name = "error", .T = Bytes(PREVIEW) },
    .{ .name = "error_truncated", .T = schema.Bool },
    // Its history: the seq the next event takes, the first not yet sent,
    // and those since, encoded, sent once a chunk is full or the run ends.
    .{ .name = "hist_next", .T = Int(u64) },
    .{ .name = "hist_flushed", .T = Int(u64) },
    .{ .name = "hist_pending", .T = Bytes(hp.HIST_CHUNK) },
    // Hand-backs, which count no failure (its history records the first
    // `max_attempts`), and deliveries past those it records
    // (`recordedDeliveries`): both said once, as it ends.
    .{ .name = "abandons", .T = Int(u32) },
    .{ .name = "deliveries_unrecorded", .T = Int(u32) },
});

/// How much of a run's result or error its row keeps.
pub const PREVIEW: u32 = 256;

pub const Empty = delivery.Empty;
pub const Deadline = delivery.Deadline;
pub const Request = delivery.Request;

/// A run's leases: the delivery model, in actions' own families, keyed by
/// namespace id.
const M = delivery.Model(.{
    .ready_id = 22,
    .ready_name = "action.ready",
    .deadline_id = 23,
    .deadline_name = "action.deadline",
    .request_id = 24,
    .request_name = "action.request",
    .scope = "action",
    .max_scope = ae.MAX_ACTION_NAME,
    .max_id = ae.MAX_RUN_ID,
    .noun = "run",
});

pub const Defs = view_mod.Family(20, "action.def", .id, Stored(Def));
pub const Runs = view_mod.Family(21, "action.run", .id, Stored(Run));
/// Every run, as its action's list orders it: newest first, then by id in
/// byte order. Written at its invoke and gone at its delete; keys alone.
pub const RunAt = view_mod.Family(74, "action.run_at", .id, delivery.Empty);

/// The key after the namespace of run `run`'s place in its action's list:
/// `len ‖ action ‖ ~created_ms ‖ run`, the id last with no length before
/// it, so the key's order is the list's.
pub fn runAtKey(buf: *RunAtBuf, action: []const u8, created_ms: i64, run: []const u8) []const u8 {
    // Its callers hold both to their bounds; past them it would overrun.
    std.debug.assert(action.len <= ae.MAX_ACTION_NAME and run.len <= ae.MAX_RUN_ID);
    std.mem.writeInt(u16, buf[0..2], @intCast(action.len), .big);
    @memcpy(buf[2..][0..action.len], action);
    std.mem.writeInt(u64, buf[2 + action.len ..][0..8], std.math.maxInt(u64) - @as(u64, @intCast(@max(created_ms, 0))), .big);
    @memcpy(buf[2 + action.len + 8 ..][0..run.len], run);
    return buf[0 .. 2 + action.len + 8 + run.len];
}

/// `len ‖ action`: every run of one action in its list.
pub fn runAtPrefix(buf: *RunAtBuf, action: []const u8) []const u8 {
    std.debug.assert(action.len <= ae.MAX_ACTION_NAME);
    std.mem.writeInt(u16, buf[0..2], @intCast(action.len), .big);
    @memcpy(buf[2..][0..action.len], action);
    return buf[0 .. 2 + action.len];
}

pub const RunAtBuf = [2 + ae.MAX_ACTION_NAME + 8 + ae.MAX_RUN_ID]u8;

/// An action's runs by where they are, kept as they move, so its info
/// reads one row whatever its runs. Keyed by the action, as its definition.
pub const CountsRec = Record(&.{
    .{ .name = "ready", .T = Int(u64) },
    .{ .name = "leased", .T = Int(u64) },
    .{ .name = "completed", .T = Int(u64) },
    .{ .name = "failed", .T = Int(u64) },
    .{ .name = "timed_out", .T = Int(u64) },
});
pub const Counts = view_mod.Family(75, "action.counts", .id, Stored(CountsRec));

const ZERO_COUNTS: CountsRec.Value = .{ .ready = 0, .leased = 0, .completed = 0, .failed = 0, .timed_out = 0 };

/// Action `action`'s runs, by where they are; all zero for one with none.
pub fn countsOf(read: view_mod.ReadView, ns: NamespaceId, action: []const u8) CountsRec.Value {
    var b: [@sizeOf(u32) + ae.MAX_ACTION_NAME]u8 = undefined;
    const k = view_mod.idKey(&b, ns, action) catch return ZERO_COUNTS;
    const got = read.get(Counts, k) catch return ZERO_COUNTS;
    return if (got) |c| c.v else ZERO_COUNTS;
}

fn countField(c: *CountsRec.Value, s: Status) *u64 {
    return switch (s) {
        .ready => &c.ready,
        .leased => &c.leased,
        .completed => &c.completed,
        .failed => &c.failed,
        .timed_out => &c.timed_out,
    };
}

/// One of action `action`'s runs moved from `from` to `to`, null for none
/// (an invoke, a delete). A count never wraps: one that would is a Flo
/// bug, which the simulator's recount catches. A row all zero after a
/// delete goes.
fn moveCount(view: *View, ns: NamespaceId, action: []const u8, from: ?Status, to: ?Status) ApplyError!void {
    if (comptime plant.active == .actions_counts_unmoved) return;
    const k = try idKey(view, ns, action);
    var c = if (try view.get(Counts, k)) |got| got.v else ZERO_COUNTS;
    if (from) |f| countField(&c, f).* -|= 1;
    if (to) |t| countField(&c, t).* +|= 1;
    if (to == null and std.meta.eql(c, ZERO_COUNTS)) return view.delete(Counts, k);
    try view.put(Counts, k, .{ .v = c });
}
pub const Ready = M.Ready;
pub const Deadlines = M.Deadlines;
pub const Requests = M.Requests;

// ─── Keys ────────────────────────────────────────────────────────────────

pub const MAX_KEY_LEN = M.MAX_KEY;
const KeyBuf = M.KeyBuf;
const ReqBuf = M.ReqBuf;

pub const readyKey = M.readyKey;
pub const readyPrefix = M.readyPrefix;
pub const deadlineKey = M.deadlineKey;
pub const DeadlineKey = M.DeadlineKey;
pub const parseDeadlineKey = M.parseDeadlineKey;
pub const pastDue = M.pastDue;
pub const pastDueAfter = M.pastDueAfter;
pub const requestKey = M.requestKey;
const after = delivery.after;
const extendedTo = delivery.extendedTo;

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    var b: KeyBuf = undefined;
    var rb: ReqBuf = undefined;
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .action_register => {
            const p = ae.Register.decodeAll(e.payload, &diag) catch return;
            try out.addIn(Defs.ID, .of(p.ns), p.name);
        },
        .action_delete => {
            const p = ae.Delete.decodeAll(e.payload, &diag) catch return;
            try out.addIn(Defs.ID, .of(p.ns), p.name);
        },
        .action_invoke => {
            const p = ae.Invoke.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Defs.ID, ns, p.action);
            try out.addIn(Runs.ID, ns, p.run);
            try out.addIn(Ready.ID, ns, readyKey(&b, p.action, {}, stamp, p.run));
            var ab: RunAtBuf = undefined;
            try out.addIn(RunAt.ID, ns, runAtKey(&ab, p.action, msOf(stamp), p.run));
            try out.addIn(Counts.ID, ns, p.action);
            // Its reserve, raised, and read by its verdict.
            try hq.addKeys(hq.ActionReserved, out, ns);
        },
        .action_deliver => {
            const p = ae.Deliver.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Defs.ID, ns, p.action);
            try out.addIn(Runs.ID, ns, p.run);
            try out.addIn(Ready.ID, ns, readyKey(&b, p.action, {}, p.due_ns, p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, after(stamp, p.lease_ms), ns, p.run));
            if (p.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, p.holder, k));
            try out.addIn(Counts.ID, ns, p.action);
            try hq.addKeys(hq.ActionReserved, out, ns);
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Runs.ID, ns, p.run);
            try out.addIn(Counts.ID, ns, p.action);
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, ns, p.run));
            if (p.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, p.holder, k));
            try hq.addKeys(hq.ActionReserved, out, ns);
        },
        .action_release => {
            const p = ae.Release.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Runs.ID, ns, p.run);
            try out.addIn(Counts.ID, ns, p.action);
            try out.addIn(Ready.ID, ns, readyKey(&b, p.action, {}, after(stamp, p.delay_ms), p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, ns, p.run));
            if (p.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, p.holder, k));
            try hq.addKeys(hq.ActionReserved, out, ns);
        },
        .action_extend => {
            const p = ae.Extend.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Runs.ID, ns, p.run);
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, ns, p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, extendedTo(stamp, p.lease_ms, p.held_since_ns), ns, p.run));
        },
        .action_expire => {
            const p = ae.Expire.decodeAll(e.payload, &diag) catch return;
            var it = p.items.iterator();
            while (it.next() catch return) |item| {
                const ns: NamespaceId = .of(item.ns);
                try out.addIn(Runs.ID, ns, item.run);
                try out.add(Deadlines.ID, "", deadlineKey(&b, item.expiry_ns, ns, item.run));
                try out.addIn(Ready.ID, ns, readyKey(&b, item.action, {}, stamp, item.run));
                if (item.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, item.holder, k));
                try out.addIn(Counts.ID, ns, item.action);
                try hq.addKeys(hq.ActionReserved, out, ns);
            }
        },
        .action_run_delete => {
            const p = ae.RunDelete.decodeAll(e.payload, &diag) catch return;
            try out.addIn(Runs.ID, .of(p.ns), p.run);
            var ab: RunAtBuf = undefined;
            try out.addIn(RunAt.ID, .of(p.ns), runAtKey(&ab, p.action, msOf(p.created_ns), p.run));
            try out.addIn(Counts.ID, .of(p.ns), p.action);
        },
        // A hand-back reads and writes nothing here.
        .action_abandon => {},
        else => {},
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────

pub const NO_ACTION = "not found: no such action";
pub const NO_RUN = "not found: no such run";
pub const RUN_EXISTS = "conflict: a run with this id exists";
pub const NOT_READY = "conflict: the run isn't ready to deliver";
pub const NAME_HAS_CONTROL = client_names.controlRefusal("an action name");
pub const NOT_ITS_LEASE = "bad request: an extend names its delivery's own lease and start";

/// Answers: register, the action's version (u32, 1 then one more each
/// register) and name; invoke, the run's id; deliver, the run's delivery
/// count (u32), which with the entry's (term, index) is the delivery id;
/// extend, the expiry it leaves (u64 ns); expire, the leases it ended
/// (u32). The rest answer nothing.
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .action_register => {
            const p = ae.Register.decodeAll(e.payload, &diag) catch return error.Halt;
            if (client_names.hasControl(p.name)) return out.refuse(.bad_request, .malformed, NAME_HAS_CONTROL);
            const k = try idKey(view, .of(p.ns), p.name);
            const old = try view.get(Defs, k);
            const version: u32 = if (old) |d| d.v.version +| 1 else 1;
            try view.put(Defs, k, .{ .v = .{ .owner = p.owner, .version = version, .lease_ms = p.lease_ms, .max_attempts = p.max_attempts, .created_ns = if (old) |d| d.v.created_ns else stamp } });
            out.putInt(u32, version);
            out.putBytes(p.name);
        },
        .action_delete => {
            const p = ae.Delete.decodeAll(e.payload, &diag) catch return error.Halt;
            const k = try idKey(view, .of(p.ns), p.name);
            if (try view.get(Defs, k) == null) return out.refuse(.not_found, .not_found, NO_ACTION);
            try view.delete(Defs, k);
        },
        .action_invoke => {
            const p = ae.Invoke.decodeAll(e.payload, &diag) catch return error.Halt;
            // A workflow step's start, sent by its call: answered to the
            // caller's slot from this entry, so a missing action is an
            // answer, not a refusal.
            const call = view.message != null;
            if (call and p.caller_workflow == null) return out.refuse(.bad_request, .malformed, CALL_HAS_NO_CALLER);
            const def = (try view.get(Defs, try idKey(view, .of(p.ns), p.action))) orelse {
                if (call) return reply(view, p, .no_action);
                return out.refuse(.not_found, .not_found, NO_ACTION);
            };
            const rk = try idKey(view, .of(p.ns), p.run);
            if (try view.get(Runs, rk)) |old| {
                // Asked again for the run it started; one a run id repeated
                // across callers (a clock stepped back) names is another's.
                if (call) return reply(view, p, if (eqlOpt(old.v.caller_run, p.caller_run) and eqlOpt(old.v.caller_workflow, p.caller_workflow)) .started else .conflict);
                return out.refuse(.conflict, .unclassified, RUN_EXISTS);
            }
            // A call's start comes as a message, which no edge holds: past
            // its namespace's share of this shard's history lane, it is told
            // so, and asks again later, so one tenant's calls don't fill the
            // lane every tenant's invokes wait on. Read before its own sends
            // are staged, and answered after the quota's arm, so a call over
            // both waits on the quota (which never fails it) rather than
            // failing a step that can't wait.
            const lane = if (call) historyHeld(view, .of(p.ns)) else null;
            // A call's start is held to the shard's share here, not at the
            // top-level gate, which never sees a channel's messages: past
            // it, the call is told so and asks again later.
            const mark = view.mark();
            var new: Run.Value = .{
                .action = p.action,
                .status = .ready,
                .input = p.input,
                .labels = p.labels,
                .source = p.source,
                .caller_run = p.caller_run,
                .caller_workflow = p.caller_workflow,
                .created_ns = stamp,
                .due_ns = stamp,
                .deliveries = 0,
                .failures = 0,
                .max_attempts = def.v.max_attempts,
                .holder = "",
                .request_key = null,
                .token = .{ .term = 0, .index = 0 },
                .lease_ms = 0,
                .delivered_ns = 0,
                .expiry_ns = 0,
                .ended = .none,
                .started_ns = 0,
                .completed_ns = 0,
                .outcome = "",
                .result = "",
                .result_truncated = false,
                .result_hash = 0,
                .@"error" = "",
                .error_truncated = false,
                .hist_next = 0,
                .hist_flushed = 0,
                .hist_pending = "",
                .abandons = 0,
                .deliveries_unrecorded = 0,
            };
            try record(view, .of(p.ns), p.run, &new, .{ .event = EV_INVOKED, .detail = p.input, .at_ms = msOf(stamp) }, .flush_on_size);
            try view.put(Runs, rk, .{ .v = new });
            // What its history may still add, staged with it: a refused
            // invoke raises nothing.
            try hq.raise(hq.ActionReserved, view, .of(p.ns), reserveOf(p.run, new));
            var b: KeyBuf = undefined;
            try view.put(Ready, try idKey(view, .of(p.ns), readyKey(&b, p.action, {}, stamp, p.run)), .{});
            // Its place in its action's list, staged with it: a call held
            // below rolls it back too.
            var ab: RunAtBuf = undefined;
            try view.put(RunAt, try idKey(view, .of(p.ns), runAtKey(&ab, p.action, msOf(stamp), p.run)), .{});
            try moveCount(view, .of(p.ns), p.action, null, .ready);
            // Past its namespace's run-history quota on this shard, judged
            // with its own reserve staged above and every earlier invoke of
            // this inbound: a direct invoke refused, a call's told
            // over_quota and asking again later. Before memory on both
            // paths: a call's memory arm follows, a direct invoke's is the
            // top-level gate after this apply.
            if (comptime plant.active != .quota_invoke_admits) if (try hq.verdict(hq.ActionReserved, view, .of(p.ns), reserveOf(p.run, new), .usage_and_reserve)) |o| {
                if (!call) return hq.refuse(out, view, .of(p.ns), o);
                try view.rollback(mark);
                view.store.quota_refusals += 1;
                var wb: [128]u8 = undefined;
                return replyWhy(view, p, .over_quota, hq.why(&wb, .of(p.ns), if (view.store.place) |pl| pl.group else 0, o));
            };
            if (lane) |h| {
                try view.rollback(mark);
                var wb: [128]u8 = undefined;
                const why = std.fmt.bufPrint(&wb, "history lane: namespace holds {d} of a {d} cap", .{ h.held, h.cap }) catch "history lane: past its share";
                return replyWhy(view, p, .over_budget, why);
            }
            if (call) if (gate.overSince(view, mark)) |o| {
                try view.rollback(mark);
                var wb: [128]u8 = undefined;
                const shard: u16 = if (view.store.place) |pl| pl.group else 0;
                const why = std.fmt.bufPrint(&wb, "shard {d} memory: {d} + {d} would pass its share {d}", .{ shard, o.use, o.delta, o.share }) catch "shard memory: past its share";
                return replyWhy(view, p, .over_budget, why);
            };
            if (call) try reply(view, p, .started);
            out.putBytes(p.run);
        },
        .action_deliver => {
            const p = ae.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
            if (try view.get(Defs, try idKey(view, .of(p.ns), p.action)) == null) return out.refuse(.not_found, .not_found, NO_ACTION);
            const rk = try idKey(view, .of(p.ns), p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            // The ready key the leader chose by must still be the run's,
            // and due by the log's clock.
            if (run.v.status != .ready or run.v.due_ns != p.due_ns or p.due_ns > stamp)
                return out.refuse(.conflict, .unclassified, NOT_READY);
            const before = reserveOf(p.run, run.v);
            try M.deliver(view, .of(p.ns), p.run, &run.v, p.holder, p.request_key, .{ .term = e.header.term, .index = e.header.index }, p.lease_ms, stamp);
            try moveCount(view, .of(p.ns), p.action, .ready, .leased);
            if (run.v.deliveries <= recordedDeliveries(run.v.max_attempts))
                try record(view, .of(p.ns), p.run, &run.v, attemptEvent(EV_DELIVERED, p.holder, msOf(stamp)), .flush_on_size)
            else
                run.v.deliveries_unrecorded +|= 1;
            try moveReserve(view, .of(p.ns), before, reserveOf(p.run, run.v));
            try view.put(Runs, rk, run);
            out.putInt(u32, run.v.deliveries);
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try idKey(view, .of(p.ns), p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.holder, p.deliveries, p.token)) {
                // A resend of the settle that applied answers as it did; a
                // second with another result is told the run has one.
                // It is held to the first by its hash, the row keeping only a
                // preview, and changes nothing: no write, and no send.
                if (sameDelivery(run.v, p.holder, p.deliveries, p.token) and run.v.ended == .settled) {
                    if (std.mem.eql(u8, run.v.outcome, p.outcome) and fnv1a64(p.result) == run.v.result_hash) {
                        if (comptime plant.active == .actions_resend_rewrites_result) {
                            try tellCaller(view, .of(p.ns), p.run, run.v, p.result);
                            try view.put(Runs, rk, run);
                        }
                        return;
                    }
                    return out.refuse(.conflict, .stale_report, OTHER_RESULT);
                }
                return stale(out, run.v, p.holder, p.deliveries, p.token);
            }
            const before = reserveOf(p.run, run.v);
            try M.endLease(view, .of(p.ns), p.run, run.v, p.holder, p.request_key, p.expiry_ns);
            try record(view, .of(p.ns), p.run, &run.v, .{ .event = EV_SETTLED, .detail = p.result, .extra = p.outcome, .at_ms = msOf(stamp) }, .hold);
            run.v.ended = .settled;
            run.v.outcome = p.outcome;
            preview(&run.v.result, &run.v.result_truncated, p.result);
            run.v.result_hash = fnv1a64(p.result);
            preview(&run.v.@"error", &run.v.error_truncated, "");
            try endRun(view, .of(p.ns), p.run, &run.v, .completed, stamp, p.result);
            try moveReserve(view, .of(p.ns), before, reserveOf(p.run, run.v));
            try view.put(Runs, rk, run);
        },
        .action_release => {
            const p = ae.Release.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try idKey(view, .of(p.ns), p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.holder, p.deliveries, p.token)) {
                const ended: Ended = if (p.abandoned) .abandoned else .released;
                if (sameDelivery(run.v, p.holder, p.deliveries, p.token) and run.v.ended == ended) return;
                return stale(out, run.v, p.holder, p.deliveries, p.token);
            }
            const before = reserveOf(p.run, run.v);
            try M.endLease(view, .of(p.ns), p.run, run.v, p.holder, p.request_key, p.expiry_ns);
            const at_ms = msOf(stamp);
            if (p.abandoned) {
                run.v.ended = .abandoned;
                // A hand-back counts no failure, so they may go on without
                // end: its first `max_attempts` are recorded, and the rest
                // only counted.
                if (run.v.abandons < run.v.max_attempts)
                    try record(view, .of(p.ns), p.run, &run.v, attemptEvent(EV_ABANDONED, p.@"error", at_ms), .flush_on_size);
                run.v.abandons +|= 1;
                preview(&run.v.@"error", &run.v.error_truncated, p.@"error");
                try M.readyAgain(view, .of(p.ns), p.run, &run.v, after(stamp, p.delay_ms));
                try moveCount(view, .of(p.ns), p.action, .leased, .ready);
            } else {
                run.v.ended = .released;
                run.v.failures +|= 1;
                const ends = !p.retry or run.v.failures >= run.v.max_attempts;
                // The failure that ends it goes whole; one retried, as a
                // preview.
                if (ends)
                    try record(view, .of(p.ns), p.run, &run.v, .{ .event = EV_RELEASED, .detail = p.@"error", .at_ms = at_ms }, .hold)
                else
                    try record(view, .of(p.ns), p.run, &run.v, attemptEvent(EV_RELEASED, p.@"error", at_ms), .flush_on_size);
                preview(&run.v.@"error", &run.v.error_truncated, p.@"error");
                if (ends) try endRun(view, .of(p.ns), p.run, &run.v, .failed, stamp, "") else {
                    try M.readyAgain(view, .of(p.ns), p.run, &run.v, after(stamp, p.delay_ms));
                    try moveCount(view, .of(p.ns), p.action, .leased, .ready);
                }
            }
            try moveReserve(view, .of(p.ns), before, reserveOf(p.run, run.v));
            try view.put(Runs, rk, run);
        },
        .action_extend => {
            const p = ae.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try idKey(view, .of(p.ns), p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.holder, p.deliveries, p.token)) return stale(out, run.v, p.holder, p.deliveries, p.token);
            if (p.lease_ms != run.v.lease_ms or p.held_since_ns != run.v.delivered_ns) return out.refuse(.bad_request, .unclassified, NOT_ITS_LEASE);
            // More than half the lease left, or held to the ceiling: the
            // lease stands, and nothing is written.
            const to = (try M.extend(view, .of(p.ns), p.run, &run.v, p.expiry_ns, stamp)) orelse return out.putInt(u64, run.v.expiry_ns);
            try view.put(Runs, rk, run);
            out.putInt(u64, to);
        },
        // A task handed back, as a message: its run's leader ends the lease
        // from its own record once it applies (`ActionsHandler.afterApply`).
        // It changes nothing here.
        .action_abandon => {},
        .action_run_delete => return applyRunDelete(view, e, out),
        .action_abandon_send => {
            // Only an await's shard makes these, from a task it holds.
            const p = ae.Abandon.decodeAll(e.payload, &diag) catch return error.Halt;
            const ns: NamespaceId = .of(p.ns);
            if (ns == .invalid) return error.Halt;
            try view.send(.user(ns), @intCast(@import("../node/router.zig").nsKeySlot(ns, p.action)), @backingInt(EntryType.action_abandon), e.payload);
        },
        .action_expire => {
            const p = ae.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
            var it = p.items.iterator();
            var ended: u32 = 0;
            while (it.next() catch return error.Halt) |item| {
                if (try expireOne(view, item, stamp)) ended += 1;
            }
            out.putInt(u32, ended);
        },
        else => return error.Halt,
    }
}

/// One expire item, past due by the log's clock: its key goes, and if it
/// is the run's live deadline, the lease ends as a failure. A key not yet
/// due is left alone. An item naming another action's run ends nothing,
/// and drops the key only if it isn't that run's live deadline: dropping
/// a live one would leave its lease with none.
fn expireOne(view: *View, item: ae.ExpireItem.Value, stamp: u64) ApplyError!bool {
    if (item.expiry_ns > stamp) return false;
    const rk = try idKey(view, .of(item.ns), item.run);
    const found = try view.get(Runs, rk);
    if (!try M.expireKey(view, .of(item.ns), item.run, item.action, item.expiry_ns, if (found) |r| r.v else null, stamp)) return false;
    var run = found.?;
    const before = reserveOf(item.run, run.v);
    try M.dropRequest(view, .of(item.ns), item.holder, item.request_key, item.run);
    run.v.ended = .expired;
    run.v.failures +|= 1;
    const ends = run.v.failures >= run.v.max_attempts;
    try record(view, .of(item.ns), item.run, &run.v, .{ .event = EV_EXPIRED, .detail = LEASE_EXPIRED, .at_ms = msOf(stamp) }, if (ends) .hold else .flush_on_size);
    preview(&run.v.@"error", &run.v.error_truncated, LEASE_EXPIRED);
    if (ends) try endRun(view, .of(item.ns), item.run, &run.v, .timed_out, stamp, "") else {
        try M.readyAgain(view, .of(item.ns), item.run, &run.v, stamp);
        try moveCount(view, .of(item.ns), run.v.action, .leased, .ready);
    }
    try moveReserve(view, .of(item.ns), before, reserveOf(item.run, run.v));
    try view.put(Runs, rk, run);
    return true;
}

/// What action run `id` may still add to its history, from its own names,
/// attempts and counters: computed from the run in each apply, never from
/// the message, so a resend that moves no counter moves nothing. 0 once it
/// has ended.
pub fn reserveOf(id: []const u8, run: Run.Value) u64 {
    const ended = switch (run.status) {
        .ready, .leased => false,
        .completed, .failed, .timed_out => true,
    };
    return history_reserve.action(run.action.len, id.len, run.max_attempts, run.deliveries, run.abandons, run.failures, ended);
}

/// Why `action.reserved` isn't its namespaces' live runs' reserves, or a
/// lower went past 0; null if neither. A VOPR rule; walks every run.
pub fn reserveRule(store: *const store_mod_top.Store, a: std.mem.Allocator) error{OutOfMemory}!?[]const u8 {
    if (store.quota_reserve_drift != 0) return "a run-history reserve lower went past zero";
    const read: view_mod.ReadView = .{ .store = @constCast(store) };
    var sums: std.AutoHashMapUnmanaged(u32, u64) = .empty;
    var it = read.range(Runs, "");
    while (it.next() catch return "a run could not be read") |item| {
        if (item.key.len <= 4) return "a run's key names no run";
        const r = reserveOf(item.key[4..], item.value.v);
        if (r == 0) continue;
        const e = try sums.getOrPut(a, std.mem.readInt(u32, item.key[0..4], .big));
        if (!e.found_existing) e.value_ptr.* = 0;
        e.value_ptr.* +|= r;
    }
    return hq.ruleAgainst(hq.ActionReserved, read, &sums);
}

/// Move `ns`'s action reserve from a run's `before` to `now`. It only
/// falls; a rise would be a Flo bug the VOPR's rule finds.
fn moveReserve(view: *View, ns: NamespaceId, before: u64, now: u64) ApplyError!void {
    if (comptime plant.active == .quota_action_end_keeps_reserve) if (now == 0) return;
    if (now > before) return hq.raise(hq.ActionReserved, view, ns, now - before);
    try hq.lower(hq.ActionReserved, view, ns, before - now);
}

/// The history lane's bytes a call's start in `ns` would wait on, as the
/// edge holds a client's (`Shard.historyWait`), from replicated state alone:
/// the lane's and the namespace's bytes, with what this apply has sent on it
/// so far, against the replicated part of the edge's cap, a quarter of the
/// bound share. With no share bound there is no replicated cap, and no
/// check: the edge's cap is node configuration, which no apply may read.
fn historyHeld(view: *View, ns: NamespaceId) ?struct { held: u64, cap: u64 } {
    const read: view_mod.ReadView = .{ .store = view.store };
    const cap = (settings.boundShare(read) orelse return null).share_bytes / 4;
    const own: u16 = if (view.place) |pl| pl.group else 0;
    var total = channels.metaOf(read, own, .history).bytes;
    var held = channels.nsBytesOf(read, own, .history, ns);
    for (view.sends.items) |snd| if (snd.flags & entry_mod.Flags.HISTORY != 0) {
        total += snd.payload.len;
        if (snd.origin.ns == ns) held += snd.payload.len;
    };
    if (total < cap) return null;
    if (held > 0 and total >= 2 * cap) return .{ .held = held, .cap = cap };
    if (shareHeld(held, total, cap)) return .{ .held = held, .cap = cap };
    return null;
}

fn eqlOpt(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

/// The answer to a call's start, to the calling workflow's slot.
fn reply(view: *View, p: ae.Invoke.Value, answer: ae.Answer) ApplyError!void {
    return replyWhy(view, p, answer, null);
}

fn replyWhy(view: *View, p: ae.Invoke.Value, answer: ae.Answer, why: ?[]const u8) ApplyError!void {
    const ns: NamespaceId = .of(p.ns);
    var buf: [ae.Reply.max_size]u8 = undefined;
    const bytes = ae.Reply.encodeInto(&buf, .{ .ns = p.ns, .slot = calls.callerSlot(ns, p.caller_workflow.?), .run = p.run, .answer = answer, .why = if (why) |w| w[0..@min(w.len, ae.MAX_SKIP_WHY)] else null }) catch return error.Halt;
    try view.send(.user(ns), calls.callerSlot(ns, p.caller_workflow.?), @backingInt(EntryType.call_reply), bytes);
}

/// A called run that ended tells its caller how, and what it returned:
/// `result`, the whole, which its row keeps only a preview of.
fn tellCaller(view: *View, ns: NamespaceId, run_id: []const u8, run: Run.Value, result: []const u8) ApplyError!void {
    if (run.source != .workflow) return;
    const workflow = run.caller_workflow orelse return;
    const status: ae.DoneStatus = switch (run.status) {
        .completed => .completed,
        .failed => .failed,
        .timed_out => .timed_out,
        .ready, .leased => return,
    };
    const done = run.status == .completed;
    const buf = view.arena.alloc(u8, ae.Done.max_size) catch return error.OutOfMemory;
    const bytes = ae.Done.encodeInto(buf, .{ .ns = ns.int(), .slot = calls.callerSlot(ns, workflow), .run = run_id, .status = status, .outcome = if (done) run.outcome else "", .result = if (done) result else "" }) catch return error.Halt;
    try view.send(.user(ns), calls.callerSlot(ns, workflow), @backingInt(EntryType.call_done), bytes);
}

pub const CALL_HAS_NO_CALLER = "bad request: a call's start names no calling workflow";

// ─── History ─────────────────────────────────────────────────────────────

pub const EV_INVOKED = "action_invoked";
pub const EV_DELIVERED = "action_delivered";
pub const EV_SETTLED = "action_settled";
pub const EV_RELEASED = "action_released";
pub const EV_ABANDONED = "action_abandoned";
pub const EV_EXPIRED = "action_expired";
pub const EV_ENDED = "action_ended";
pub const LEASE_EXPIRED = "lease expired";
/// An attempt's event's `extra` when its detail is a preview.
pub const TRUNCATED = "truncated";

/// `action_ended`'s detail: the hand-backs and the deliveries its history
/// didn't record. The longest, both at u32's largest, is `ENDED_DETAIL_MAX`.
pub const ENDED_FMT = "{{\"abandons\":{d},\"deliveries_unrecorded\":{d}}}";
pub const ENDED_DETAIL_MAX = std.fmt.count(ENDED_FMT, .{ std.math.maxInt(u32), std.math.maxInt(u32) });

/// A run's deliveries its history records: its history is then bounded by
/// its attempts, whatever is handed back.
pub fn recordedDeliveries(max_attempts: u32) u64 {
    return 2 * @as(u64, max_attempts);
}

comptime {
    const rb = hp.recordBound;
    const C = he.MAX_CHUNK_RECORDS;
    std.debug.assert(ae.MAX_INPUT <= hp.MAX_DETAIL and action_ops.MAX_OUTPUT <= hp.MAX_DETAIL and action_ops.MAX_ERROR <= hp.MAX_DETAIL);
    std.debug.assert(ae.MAX_OUTCOME <= hp.MAX_EXTRA);
    std.debug.assert(TRUNCATED.len <= hp.MAX_EXTRA and PREVIEW <= hp.MAX_DETAIL);
    // A run's history is owned by its action, by name: every name fits.
    std.debug.assert(ae.MAX_ACTION_NAME <= he.MAX_OWNER);
    // An event over half a chunk goes alone: an invoke's input, at most.
    std.debug.assert(rb(EV_INVOKED.len, ae.MAX_INPUT, 0) <= C);
    std.debug.assert(rb(EV_RELEASED.len, action_ops.MAX_ERROR, 0) <= C and rb(EV_ABANDONED.len, action_ops.MAX_ERROR, 0) <= C);
    // The apply that ends a run sends one chunk: its pending events, that
    // apply's and its end's.
    const ended = rb(EV_ENDED.len, ENDED_DETAIL_MAX, 0);
    const settled = rb(EV_SETTLED.len, action_ops.MAX_OUTPUT, ae.MAX_OUTCOME);
    const expired = rb(EV_EXPIRED.len, LEASE_EXPIRED.len, 0);
    std.debug.assert(hp.HIST_CHUNK + settled + ended <= C);
    std.debug.assert(hp.HIST_CHUNK + rb(EV_RELEASED.len, action_ops.MAX_ERROR, 0) + ended <= C);
    std.debug.assert(hp.HIST_CHUNK + expired + ended <= C);
    // A settle sends that chunk and its call's end.
    std.debug.assert(hp.chunkBound(hp.HIST_CHUNK + settled + ended) + ae.Done.max_size <= channels.MAX_SEND_BYTES);
    // An expire, per item it ends: a chunk, and a call's end with no
    // result (the run didn't complete); one that doesn't end it, at most
    // its pending chunk. Bounded here, whatever its proposer read.
    const item = hp.chunkBound(hp.HIST_CHUNK + expired + ended) + (ae.Done.max_size - action_ops.MAX_OUTPUT);
    std.debug.assert(2 * ae.MAX_EXPIRE_ITEMS <= channels.MAX_SENDS);
    std.debug.assert(ae.MAX_EXPIRE_ITEMS * item <= channels.MAX_SEND_BYTES);
}

fn msOf(stamp_ns: u64) i64 {
    return @intCast(stamp_ns / std.time.ns_per_ms);
}

fn histOf(run: *const Run.Value) hp.Hist {
    return .{ .next = run.hist_next, .flushed = run.hist_flushed, .pending = run.hist_pending };
}

fn setHist(run: *Run.Value, h: hp.Hist) void {
    run.hist_next = h.next;
    run.hist_flushed = h.flushed;
    run.hist_pending = h.pending;
}

/// Where run `id`'s chunks go: its action's slot, which its run id
/// carries, so its own group.
fn ownerOf(ns: NamespaceId, id: []const u8, run: *const Run.Value) hp.Owner {
    return .{ .family = .action, .ns = ns, .owner = run.action, .run = id, .created_ns = run.created_ns, .slot = @intCast(router.nsKeySlot(ns, run.action)) };
}

/// One event onto run `id`'s history. A run that ended records none: an
/// entry that would is a Flo bug.
fn record(view: *View, ns: NamespaceId, id: []const u8, run: *Run.Value, ev: hp.Event, mode: hp.Mode) ApplyError!void {
    if (run.completed_ns != 0) return error.Halt;
    var h = histOf(run);
    try hp.append(view, ownerOf(ns, id, run), &h, ev, mode);
    setHist(run, h);
}

/// An event of one attempt that doesn't end its run: its worker or error
/// as a preview, `extra` saying when it was cut. With its deliveries and
/// hand-backs bounded by its attempts, a run's history is then bounded by
/// its attempts and its invoke and end, whatever its workers report.
fn attemptEvent(name: []const u8, whole: []const u8, at_ms: i64) hp.Event {
    const cut = whole.len > PREVIEW;
    return .{ .event = name, .detail = whole[0..@min(whole.len, PREVIEW)], .extra = if (cut) TRUNCATED else "", .at_ms = at_ms };
}

/// `field` set to `whole`'s preview.
fn preview(field: *[]const u8, truncated: *bool, whole: []const u8) void {
    field.* = whole[0..@min(whole.len, PREVIEW)];
    truncated.* = whole.len > PREVIEW;
}

/// Run `id` ended `status` at `stamp`, this apply's events held for it:
/// the counts its history didn't record go in its last chunk, sent now;
/// its caller is told, with the whole `result`; and its row keeps its
/// summary, without its input or labels.
fn endRun(view: *View, ns: NamespaceId, id: []const u8, run: *Run.Value, status: Status, stamp: u64, result: []const u8) ApplyError!void {
    var db: [ENDED_DETAIL_MAX]u8 = undefined;
    const detail = std.fmt.bufPrint(&db, ENDED_FMT, .{ run.abandons, run.deliveries_unrecorded }) catch unreachable;
    try record(view, ns, id, run, .{ .event = EV_ENDED, .detail = detail, .at_ms = msOf(stamp) }, .hold);
    try moveCount(view, ns, run.action, run.status, status);
    run.status = status;
    run.completed_ns = stamp;
    try tellCaller(view, ns, id, run.*, result);
    if (comptime plant.active != .actions_end_keeps_input) run.input = "";
    run.labels = null;
    var h = histOf(run);
    try hp.flushKeeping(view, ownerOf(ns, id, run), &h, msOf(stamp), comptime plant.active == .actions_hist_flushed_kept);
    setHist(run, h);
}

/// The run at `rk`, if it is `action`'s.
fn inScope(view: *View, rk: []const u8, action: []const u8) ApplyError!?Stored(Run) {
    const run = (try view.get(Runs, rk)) orelse return null;
    return if (std.mem.eql(u8, run.v.action, action)) run else null;
}

pub const OTHER_RESULT = "stale report: the run was completed with another result";
/// An action run has no cancel: it ends by its worker or its attempts.
pub const RUN_LIVE = "bad request: the run hasn't ended; delete it once it ends";

/// An ended run deleted: its row goes, and its history is dropped on the
/// history lane, after its last chunk. Its id is then free for a new run.
/// No caller asks for it again: a call asks only while no run exists,
/// and call run ids are never chosen twice. Answers `not_found` for a run
/// that isn't there, a delete resent included, or another incarnation.
fn applyRunDelete(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const p = ae.RunDelete.decodeAll(e.payload, &diag) catch return error.Halt;
    const ns: NamespaceId = .of(p.ns);
    const rk = try idKey(view, ns, p.run);
    const run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
    if (run.v.created_ns != p.created_ns) return out.refuse(.not_found, .not_found, NO_RUN);
    if (run.v.completed_ns == 0) return out.refuse(.bad_request, .run_live, RUN_LIVE);
    try view.delete(Runs, rk);
    var ab: RunAtBuf = undefined;
    try view.delete(RunAt, try idKey(view, ns, runAtKey(&ab, run.v.action, msOf(run.v.created_ns), p.run)));
    try moveCount(view, ns, run.v.action, run.v.status, null);
    if (comptime plant.active == .actions_delete_keeps_history) return;
    try hp.sendDrop(view, ownerOf(ns, p.run, &run.v), msOf(run.v.completed_ns));
}

const holds = delivery.holds;
const sameDelivery = delivery.sameDelivery;
const stale = M.stale;
pub const staleWhy = M.staleWhy;
/// `k` in `ns`; an entry naming no namespace (`.invalid`) halts.
const idKey = M.inNs;

pub const machine: machine_mod.Machine = .{
    .name = "actions",
    .entry_types = &.{ .action_register, .action_delete, .action_invoke, .action_deliver, .action_settle, .action_release, .action_extend, .action_expire, .action_run_delete, .action_abandon_send, .action_abandon },
    .families = &([_]machine_mod.FamilyInfo{ machine_mod.info(Defs), machine_mod.info(Runs), machine_mod.info(RunAt), machine_mod.info(Counts), machine_mod.info(hq.ActionReserved) } ++ M.FAMILIES),
    .keys = keys,
    .apply = apply,
    .skipped = skipped,
    .malformed = malformed,
    .namespace = namespaceOf,
};

/// A hand-back that doesn't decode was built wrong by its sender. Only a
/// hand-back is checked: no other type here comes over a channel this way.
fn malformed(e: *const Entry) ?[]const u8 {
    if (typeOf(e) != .action_abandon) return null;
    var diag: schema.Diagnostic = .{};
    _ = ae.Abandon.decodeAll(e.payload, &diag) catch return "bad request: a hand-back that doesn't decode";
    return null;
}

/// A hand-back's namespace, held to its message's origin; null for every
/// other type, whose keys say it.
fn namespaceOf(e: *const Entry) ?u32 {
    if (typeOf(e) != .action_abandon) return null;
    var diag: schema.Diagnostic = .{};
    const p = ae.Abandon.decodeAll(e.payload, &diag) catch return null;
    return p.ns;
}

/// A call's start a repair skipped: its caller is told, so the step fails
/// saying so. A start no workflow made has nobody waiting.
fn skipped(view: *View, e: *const Entry, why: []const u8) ApplyError!void {
    if (e.header.entry_type != @backingInt(EntryType.action_invoke)) return;
    var diag: schema.Diagnostic = .{};
    const p = ae.Invoke.decodeAll(e.payload, &diag) catch return;
    if (p.caller_workflow == null) return;
    try replyWhy(view, p, .skipped, why);
}

// ─── Tests ───────────────────────────────────────────────────────────────

/// Action state on its own, for tests: entries applied through the
/// machine as a shard applies them, one index after another, and kept as
/// a log another store can replay.
pub const TestActions = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    log: std.ArrayListUnmanaged(Logged) = .empty,
    index: u64 = 0,
    term: u64 = 1,

    const store_mod = @import("../contract/store.zig");

    const Logged = struct { etype: EntryType, term: u64, index: u64, stamp: u64, payload: []u8 };

    pub fn init(allocator: std.mem.Allocator) TestActions {
        var store = store_mod.Store.init(allocator);
        store.strict_keys = true;
        // A group of one: every slot is its own, its runs' history too.
        store.place = .{ .group = 0, .count = 1 };
        return .{ .store = store, .scratch = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *TestActions) void {
        for (self.log.items) |l| self.store.allocator.free(l.payload);
        self.log.deinit(self.store.allocator);
        self.store.deinit();
        self.scratch.deinit();
    }

    /// `body` as the next entry of type `etype`, stamped `stamp_ns`.
    pub fn apply(self: *TestActions, comptime etype: EntryType, body: anytype, stamp_ns: u64) !Result {
        const P = ae.Payload(@backingInt(etype));
        const payload = try self.store.allocator.alloc(u8, try P.encodedSize(body));
        _ = try P.encodeInto(payload, body);
        self.index += 1;
        try self.log.append(self.store.allocator, .{ .etype = etype, .term = self.term, .index = self.index, .stamp = stamp_ns, .payload = payload });
        return self.applyLogged(self.log.items[self.log.items.len - 1]);
    }

    fn applyLogged(self: *TestActions, l: Logged) !Result {
        const e = entry_mod.buildEntry(l.etype, entry_mod.Flags.NONE, l.term, l.index, l.stamp, l.payload);
        var out: Result = .{};
        out.reset(&e);
        try machine_mod.applyEntry(&machine, &self.store, &self.scratch, &e, &out);
        return out;
    }

    /// Every entry of `self`'s log, applied to `other` as it came.
    pub fn replayInto(self: *TestActions, other: *TestActions) !void {
        for (self.log.items) |l| _ = try other.applyLogged(l);
    }

    pub fn run(self: *TestActions, ns: NamespaceId, id: []const u8) ?Run.Value {
        var b: [4 + ae.MAX_RUN_ID]u8 = undefined;
        const k = view_mod.idKey(&b, ns, id) catch unreachable;
        const got = (view_mod.ReadView{ .store = &self.store }).get(Runs, k) catch unreachable;
        return if (got) |r| r.v else null;
    }

    /// Every family's checksum: equal before and after means nothing changed.
    pub fn sums(self: *TestActions) [5]u64 {
        var out: [5]u64 = undefined;
        inline for (.{ Defs, Runs, Ready, Deadlines, Requests }, 0..) |F, i| out[i] = store_mod.checksum(&self.store, F.ID);
        return out;
    }

    pub fn count(self: *TestActions, comptime F: type) usize {
        var n: usize = 0;
        var it = self.store.range(F.ID, "");
        while (it.next()) |_| n += 1;
        return n;
    }

    /// The expire duty's tick at `stamp`: one expire naming every key
    /// past due, as the leader proposes it.
    pub fn duty(self: *TestActions, stamp: u64) !void {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.store.allocator);
        var n: u32 = 0;
        var it = self.store.range(Deadlines.ID, "");
        while (it.next()) |rec| {
            const k = parseDeadlineKey(rec.key()[1..]).?;
            if (k.expiry_ns > stamp) break;
            const d = try Stored(Deadline).decode(rec.value);
            var ib: [ae.ExpireItem.max_size]u8 = undefined;
            try buf.appendSlice(self.store.allocator, try ae.ExpireItem.encodeInto(&ib, .{ .ns = k.ns.int(), .action = d.v.scope, .run = k.id, .expiry_ns = k.expiry_ns, .holder = "w1", .request_key = null }));
            n += 1;
        }
        if (n == 0) return;
        _ = try self.apply(.action_expire, ae.Expire.Value{ .items = .{ .bytes = buf.items, .count = n } }, stamp);
    }
};

const testing = std.testing;
const Reason = @import("../protocol/proto.zig").Reason;
const ms = std.time.ns_per_ms;
const sec = std.time.ns_per_s;
/// Every test lease: the floor.
const LEASE_MS = ae.MIN_LEASE_MS;
const LEASE = LEASE_MS * ms;
/// The default namespace's id, as an entry carries it.
const DEFAULT = NamespaceId.default.int();

fn registerIn(t: *TestActions, ns: NamespaceId, max_attempts: u32) !void {
    const r = try t.apply(.action_register, ae.Register.Value{ .ns = ns.int(), .name = "send", .owner = "", .lease_ms = LEASE_MS, .max_attempts = max_attempts }, 1);
    try testing.expect(r.refused() == null);
}

fn register(t: *TestActions, max_attempts: u32) !void {
    return registerIn(t, .default, max_attempts);
}

fn invokeIn(t: *TestActions, ns: NamespaceId, run_id: []const u8, stamp: u64) !void {
    const r = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = ns.int(), .action = "send", .run = run_id, .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, stamp);
    try testing.expect(r.refused() == null);
}

fn invoke(t: *TestActions, run_id: []const u8, stamp: u64) !void {
    return invokeIn(t, .default, run_id, stamp);
}

fn deliverIn(t: *TestActions, ns: NamespaceId, run_id: []const u8, due: u64, stamp: u64) !Result {
    return t.apply(.action_deliver, ae.Deliver.Value{ .ns = ns.int(), .action = "send", .run = run_id, .due_ns = due, .holder = "w1", .request_key = null, .lease_ms = LEASE_MS }, stamp);
}

fn deliver(t: *TestActions, run_id: []const u8, due: u64, stamp: u64) !Result {
    return deliverIn(t, .default, run_id, due, stamp);
}

/// A settle of the run's live delivery, as its leader would propose it.
fn settleOf(t: *TestActions, run_id: []const u8) ae.Settle.Value {
    const r = t.run(.default, run_id).?;
    return .{ .ns = DEFAULT, .action = "send", .run = run_id, .holder = "w1", .request_key = null, .deliveries = r.deliveries, .token = r.token, .expiry_ns = r.expiry_ns, .outcome = "", .result = "done" };
}

fn releaseOf(t: *TestActions, run_id: []const u8, abandoned: bool) ae.Release.Value {
    const r = t.run(.default, run_id).?;
    return .{ .ns = DEFAULT, .action = "send", .run = run_id, .holder = "w1", .request_key = null, .deliveries = r.deliveries, .token = r.token, .expiry_ns = r.expiry_ns, .abandoned = abandoned, .retry = true, .delay_ms = 5, .@"error" = "boom" };
}

fn extendOf(t: *TestActions, run_id: []const u8) ae.Extend.Value {
    const r = t.run(.default, run_id).?;
    return .{ .ns = DEFAULT, .action = "send", .run = run_id, .holder = "w1", .deliveries = r.deliveries, .token = r.token, .expiry_ns = r.expiry_ns, .held_since_ns = r.delivered_ns, .lease_ms = r.lease_ms };
}

/// An expire of `item` alone.
fn expiring(buf: []u8, item: ae.ExpireItem.Value) !ae.Expire.Value {
    return .{ .items = .{ .bytes = try ae.ExpireItem.encodeInto(buf, item), .count = 1 } };
}

fn keyOf(run_id: []const u8, expiry_ns: u64) ae.ExpireItem.Value {
    return .{ .ns = DEFAULT, .action = "send", .run = run_id, .expiry_ns = expiry_ns, .holder = "w1", .request_key = null };
}

test "actions machine: a register naming a control character is refused at apply, saying so, and a UTF-8 name is not" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    for ([_][]const u8{ "se\x00nd", "se\nnd", "se\x1bnd", "se\x7fnd" }) |name| {
        const r = try t.apply(.action_register, ae.Register.Value{ .ns = DEFAULT, .name = name, .owner = "", .lease_ms = LEASE_MS, .max_attempts = 1 }, 1);
        try testing.expectEqualStrings(NAME_HAS_CONTROL, r.refused().?.refusal.message);
    }
    const ok = try t.apply(.action_register, ae.Register.Value{ .ns = DEFAULT, .name = "s\xc3\xa9nd", .owner = "", .lease_ms = LEASE_MS, .max_attempts = 1 }, 1);
    try testing.expect(ok.refused() == null);
}

test "actions machine: a run is delivered once, settled by its holder, and a resent settle answers as it did" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expectEqual(@as(usize, 1), t.count(Ready));

    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const leased = t.run(.default, "r1").?;
    try testing.expectEqual(Status.leased, leased.status);
    try testing.expectEqual(@as(u32, 1), leased.deliveries);
    try testing.expectEqual(20 * ms + LEASE, leased.expiry_ns);
    try testing.expectEqual(@as(usize, 0), t.count(Ready));
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    // The same ready key can't be delivered twice.
    try testing.expect((try deliver(&t, "r1", 10 * ms, 21 * ms)).refused() != null);

    // A worker's "failure" completes the run: the workflow follows it.
    var settle = settleOf(&t, "r1");
    settle.outcome = "failure";
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    const done = t.run(.default, "r1").?;
    try testing.expectEqual(Status.completed, done.status);
    try testing.expectEqualStrings("failure", done.outcome);
    // The settle took its deadline key with it.
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
    const before = t.sums();
    try testing.expect((try t.apply(.action_settle, settle, 31 * ms)).refused() == null);
    try testing.expectEqual(before, t.sums());
}

test "actions machine: a run reserves its quota share at invoke; each delivery, failure and hand-back moves it by what its counters left; its end releases it all, and a resend moves nothing" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    const read: view_mod.ReadView = .{ .store = &t.store };
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    const fresh = t.run(.default, "r1").?;
    try testing.expectEqual(history_reserve.action(4, 2, 3, 0, 0, 0, false), reserveOf("r1", fresh));
    try testing.expectEqual(reserveOf("r1", fresh), hq.reservedOf(hq.ActionReserved, read, .default));
    // Each counter that moves takes what it can no longer add.
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    try testing.expectEqual(reserveOf("r1", t.run(.default, "r1").?), hq.reservedOf(hq.ActionReserved, read, .default));
    try testing.expect(hq.reservedOf(hq.ActionReserved, read, .default) < reserveOf("r1", fresh));
    try testing.expect((try t.apply(.action_release, releaseOf(&t, "r1", false), 30 * ms)).refused() == null);
    try testing.expectEqual(reserveOf("r1", t.run(.default, "r1").?), hq.reservedOf(hq.ActionReserved, read, .default));
    try testing.expect((try deliver(&t, "r1", t.run(.default, "r1").?.due_ns, 40 * ms)).refused() == null);
    try testing.expect((try t.apply(.action_release, releaseOf(&t, "r1", true), 50 * ms)).refused() == null);
    try testing.expectEqual(reserveOf("r1", t.run(.default, "r1").?), hq.reservedOf(hq.ActionReserved, read, .default));
    // A second run's reserve adds to the first's.
    try invoke(&t, "r2", 60 * ms);
    try testing.expectEqual(reserveOf("r1", t.run(.default, "r1").?) + reserveOf("r2", t.run(.default, "r2").?), hq.reservedOf(hq.ActionReserved, read, .default));
    // r1's settle ends it: its reserve goes, and a resend moves nothing.
    try testing.expect((try deliver(&t, "r1", t.run(.default, "r1").?.due_ns, 70 * ms)).refused() == null);
    const settle = settleOf(&t, "r1");
    try testing.expect((try t.apply(.action_settle, settle, 80 * ms)).refused() == null);
    try testing.expectEqual(@as(u64, 0), reserveOf("r1", t.run(.default, "r1").?));
    try testing.expectEqual(reserveOf("r2", t.run(.default, "r2").?), hq.reservedOf(hq.ActionReserved, read, .default));
    try testing.expect((try t.apply(.action_settle, settle, 81 * ms)).refused() == null);
    try testing.expectEqual(reserveOf("r2", t.run(.default, "r2").?), hq.reservedOf(hq.ActionReserved, read, .default));
    // r2 expires to its end: 3 attempts, each delivered and its lease run out.
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    var at: u64 = 90 * ms;
    for (0..3) |_| {
        try testing.expect((try deliver(&t, "r2", t.run(.default, "r2").?.due_ns, at)).refused() == null);
        const expiry = t.run(.default, "r2").?.expiry_ns;
        _ = try t.apply(.action_expire, try expiring(&ibuf, keyOf("r2", expiry)), expiry + 1);
        at = expiry + 2;
    }
    try testing.expectEqual(Status.timed_out, t.run(.default, "r2").?.status);
    try testing.expectEqual(@as(u64, 0), hq.reservedOf(hq.ActionReserved, read, .default));
    var kb: [4]u8 = undefined;
    try testing.expect((try read.get(hq.ActionReserved, view_mod.idKey(&kb, .default, "") catch unreachable)) == null);
    try testing.expectEqual(@as(u64, 0), t.store.quota_reserve_drift);
}

test "actions machine: a refused entry changes nothing: a stale token, an early deliver, another scope, an expire before its deadline" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);

    // Not yet due by the log's clock, or chosen by a key that isn't the run's.
    var before = t.sums();
    try testing.expect((try deliver(&t, "r1", 10 * ms, 5 * ms)).refused() != null);
    try testing.expect((try deliver(&t, "r1", 11 * ms, 20 * ms)).refused() != null);
    try testing.expectEqual(before, t.sums());

    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    before = t.sums();
    var wrong = settleOf(&t, "r1");
    wrong.token.index += 1;
    try testing.expectEqual(Reason.stale_report, (try t.apply(.action_settle, wrong, 30 * ms)).refused().?.refusal.reason);
    var ext = extendOf(&t, "r1");
    ext.token.index += 1;
    try testing.expect((try t.apply(.action_extend, ext, 30 * ms)).refused() != null);
    // An extend names its delivery's count, as a settle does: another
    // count is another delivery's, refused at apply on every replica.
    ext = extendOf(&t, "r1");
    ext.deliveries += 1;
    try testing.expectEqual(Reason.stale_report, (try t.apply(.action_extend, ext, 30 * ms)).refused().?.refusal.reason);
    try testing.expectEqual(before, t.sums());

    // Another action's name with the live token: answered as no such run,
    // never a halt.
    var settle = settleOf(&t, "r1");
    settle.action = "other";
    try testing.expectEqual(Reason.not_found, (try t.apply(.action_settle, settle, 30 * ms)).refused().?.refusal.reason);
    var rel = releaseOf(&t, "r1", false);
    rel.action = "other";
    try testing.expectEqual(Reason.not_found, (try t.apply(.action_release, rel, 30 * ms)).refused().?.refusal.reason);
    ext = extendOf(&t, "r1");
    ext.action = "other";
    try testing.expectEqual(Reason.not_found, (try t.apply(.action_extend, ext, 30 * ms)).refused().?.refusal.reason);
    try testing.expectEqual(before, t.sums());

    // An expire before the deadline, or naming another action, leaves the
    // lease and its key.
    const expiry = t.run(.default, "r1").?.expiry_ns;
    _ = try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", expiry)), expiry - 1);
    var other = keyOf("r1", expiry);
    other.action = "other";
    _ = try t.apply(.action_expire, try expiring(&ibuf, other), expiry + 1);
    try testing.expectEqual(before, t.sums());
}

test "actions machine: failures count to max_attempts; an abandoned delivery counts none; neither leaves a key" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 2);
    try invoke(&t, "r1", 10 * ms);

    var due: u64 = 10 * ms;
    var stamp: u64 = 20 * ms;
    for ([_]bool{ true, false, false }) |abandon| {
        try testing.expect((try deliver(&t, "r1", due, stamp)).refused() == null);
        const rel = releaseOf(&t, "r1", abandon);
        try testing.expect((try t.apply(.action_release, rel, stamp + ms)).refused() == null);
        try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
        // A resend answers as the first did, and changes nothing.
        const before = t.sums();
        try testing.expect((try t.apply(.action_release, rel, stamp + 2 * ms)).refused() == null);
        try testing.expectEqual(before, t.sums());
        // A settle or the other kind of release for that delivery is
        // refused: it ended otherwise.
        const settle: ae.Settle.Value = .{ .ns = DEFAULT, .action = "send", .run = "r1", .holder = rel.holder, .request_key = null, .deliveries = rel.deliveries, .token = rel.token, .expiry_ns = rel.expiry_ns, .outcome = "", .result = "" };
        try testing.expectEqualStrings("stale report: the run was released", (try t.apply(.action_settle, settle, stamp + 2 * ms)).refused().?.refusal.message);
        var other = rel;
        other.abandoned = !abandon;
        try testing.expect((try t.apply(.action_release, other, stamp + 2 * ms)).refused() != null);
        try testing.expectEqual(before, t.sums());
        due = stamp + ms + 5 * ms;
        stamp = due + ms;
    }
    const done = t.run(.default, "r1").?;
    try testing.expectEqual(Status.failed, done.status);
    try testing.expectEqual(@as(u32, 3), done.deliveries);
    try testing.expectEqual(@as(u32, 2), done.failures);
}

test "actions machine: a leader's abandon proposed again after the expire applied changes nothing, and the run is delivered once more, not twice" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    // The hand-back a refused propose kept, as it was first proposed.
    var back = releaseOf(&t, "r1", true);
    back.delay_ms = 0;
    back.@"error" = "";
    const live = t.run(.default, "r1").?.expiry_ns;
    try testing.expect((try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", live)), live + ms)).refused() == null);
    const sums = t.sums();
    try testing.expectEqualStrings("stale report: the lease expired and the run moved on", (try t.apply(.action_release, back, live + 2 * ms)).refused().?.refusal.message);
    try testing.expectEqual(sums, t.sums());
    try testing.expectEqual(@as(u32, 1), t.run(.default, "r1").?.failures);
    // Delivered again once; the kept hand-back still applies nothing.
    try testing.expect((try deliver(&t, "r1", live + ms, live + 3 * ms)).refused() == null);
    const again = t.sums();
    try testing.expectEqualStrings("stale report: the run was delivered again since", (try t.apply(.action_release, back, live + 4 * ms)).refused().?.refusal.message);
    try testing.expectEqual(again, t.sums());
    const run = t.run(.default, "r1").?;
    try testing.expectEqual(Status.leased, run.status);
    try testing.expectEqual(@as(u32, 2), run.deliveries);
}

test "actions machine: 500 deliver and abandon cycles leave no deadline key" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    var due: u64 = 10 * ms;
    for (0..500) |_| {
        const stamp = due + ms;
        try testing.expect((try deliver(&t, "r1", due, stamp)).refused() == null);
        try testing.expect((try t.apply(.action_release, releaseOf(&t, "r1", true), stamp + ms)).refused() == null);
        due = stamp + ms + 5 * ms;
    }
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
    try testing.expectEqual(@as(u32, 0), t.run(.default, "r1").?.failures);
}

test "actions machine: touches every millisecond, each racing an extend, keep at most three keys for the lease" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 1);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);

    // Every extend names the delivery's first expiry, as if each raced
    // another in flight, so no extend deletes the key it replaces.
    const first = extendOf(&t, "r1");
    var stamp: u64 = 21 * ms;
    var most: usize = 0;
    while (stamp < 120 * sec) : (stamp += ms) {
        var ext = extendOf(&t, "r1");
        ext.expiry_ns = first.expiry_ns;
        try testing.expect((try t.apply(.action_extend, ext, stamp)).refused() == null);
        try t.duty(stamp);
        most = @max(most, t.count(Deadlines));
    }
    try testing.expect(most <= 3);
    try testing.expectEqual(Status.leased, t.run(.default, "r1").?.status);

    // An extend asking another lease, or naming another start, is refused.
    var other = extendOf(&t, "r1");
    other.lease_ms = LEASE_MS + 1;
    try testing.expect((try t.apply(.action_extend, other, stamp)).refused() != null);
    other = extendOf(&t, "r1");
    other.held_since_ns += 1;
    try testing.expect((try t.apply(.action_extend, other, stamp)).refused() != null);
}

test "actions machine: a delivery touched forever is held no longer than the ceiling" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 1);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const ceiling = t.run(.default, "r1").?.delivered_ns + ae.MAX_HOLD_MS * ms;
    var stamp: u64 = 21 * ms;
    while (stamp < ceiling + 10 * LEASE) : (stamp += LEASE / 4) {
        const r = t.run(.default, "r1").?;
        if (r.status != .leased) break;
        _ = try t.apply(.action_extend, extendOf(&t, "r1"), stamp);
        try t.duty(stamp);
        try testing.expect(t.count(Deadlines) <= 1);
        try testing.expect(t.run(.default, "r1").?.expiry_ns <= ceiling);
    }
    // Its last extend reached the ceiling, and the lease ran out there.
    const r = t.run(.default, "r1").?;
    try testing.expectEqual(Status.timed_out, r.status);
    try testing.expectEqual(ceiling, r.expiry_ns);
}

test "actions machine: an extend to the same instant keeps one deadline, and the lease still expires at it" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 1);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    // Two extends at one stamp: the second lands on the key the first
    // wrote, naming the expiry before both.
    const stale_seen = extendOf(&t, "r1").expiry_ns;
    const at = 20 * ms + LEASE / 2;
    try testing.expect((try t.apply(.action_extend, extendOf(&t, "r1"), at)).refused() == null);
    var again = extendOf(&t, "r1");
    again.expiry_ns = stale_seen;
    // More than half is left now, so the second writes nothing.
    try testing.expect((try t.apply(.action_extend, again, at)).refused() == null);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    const expiry = t.run(.default, "r1").?.expiry_ns;
    try testing.expectEqual(at + LEASE, expiry);
    _ = try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", expiry)), expiry);
    try testing.expectEqual(Status.timed_out, t.run(.default, "r1").?.status);
}

test "actions machine: an expire drops a stale key, and ends a live lease with attempts left by readying it at the expire's stamp" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const old = t.run(.default, "r1").?.expiry_ns;
    // An extend that missed the expiry it named leaves the old key behind.
    var ext = extendOf(&t, "r1");
    ext.expiry_ns = 0;
    try testing.expect((try t.apply(.action_extend, ext, 20 * ms + LEASE * 3 / 4)).refused() == null);
    try testing.expectEqual(@as(usize, 2), t.count(Deadlines));

    // Named with another action, the stale key still goes, so a duty
    // that misnames it doesn't name it again every tick.
    var misnamed = keyOf("r1", old);
    misnamed.action = "other";
    _ = try t.apply(.action_expire, try expiring(&ibuf, misnamed), old + ms);
    try testing.expectEqual(Status.leased, t.run(.default, "r1").?.status);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    const live = t.run(.default, "r1").?.expiry_ns;
    const stamp = live + 7 * ms;
    try testing.expect((try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", live)), stamp)).refused() == null);
    const run = t.run(.default, "r1").?;
    try testing.expectEqual(Status.ready, run.status);
    try testing.expectEqual(Ended.expired, run.ended);
    try testing.expectEqual(@as(u32, 1), run.failures);
    try testing.expectEqual(stamp, run.due_ns);
    try testing.expectEqual(@as(usize, 1), t.count(Ready));
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
    // Redelivered at that due, and not before it.
    try testing.expect((try deliver(&t, "r1", stamp, stamp - 1)).refused() != null);
    try testing.expect((try deliver(&t, "r1", stamp, stamp)).refused() == null);

    // The last holder's late settle is told what happened.
    var late = settleOf(&t, "r1");
    late.deliveries = 1;
    try testing.expectEqualStrings("stale report: the run was delivered again since", (try t.apply(.action_settle, late, stamp + ms)).refused().?.refusal.message);
}

test "actions machine: a settle after the expiry instant but before the expire is accepted: log order decides" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const expiry = t.run(.default, "r1").?.expiry_ns;
    try testing.expect((try t.apply(.action_settle, settleOf(&t, "r1"), expiry + ms)).refused() == null);
    const r = try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", expiry)), expiry + 2 * ms);
    var answer = r.reader();
    try testing.expectEqual(@as(u32, 0), answer.int(u32).?);
    try testing.expectEqual(Status.completed, t.run(.default, "r1").?.status);
}

test "actions machine: one run id in two namespaces has two deadline keys" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    for ([_]NamespaceId{ .default, .of(16) }) |ns| {
        try registerIn(&t, ns, 1);
        try invokeIn(&t, ns, "r1", 10 * ms);
        try testing.expect((try deliverIn(&t, ns, "r1", 10 * ms, 20 * ms)).refused() == null);
    }
    try testing.expectEqual(@as(usize, 2), t.count(Deadlines));
}

test "actions machine: one action and run id in two namespaces are two of each, by the keys an entry declares and the state it leaves" {
    const other: NamespaceId = .of(16);
    // The keys an entry declares, built as a shard builds them: a register
    // in one namespace covers its own definition, never the other's.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var pb: [ae.Register.max_size]u8 = undefined;
    const payload = try ae.Register.encodeInto(&pb, .{ .ns = DEFAULT, .name = "send", .owner = "", .lease_ms = LEASE_MS, .max_attempts = 1 });
    const e = entry_mod.buildEntry(.action_register, entry_mod.Flags.NONE, 1, 1, 1, payload);
    var declared = KeySet.init(arena.allocator());
    try keys(&e, &declared);
    var kb: [4 + ae.MAX_ACTION_NAME]u8 = undefined;
    try testing.expect(declared.covers(Defs.ID, try view_mod.idKey(&kb, .default, "send")));
    try testing.expect(!declared.covers(Defs.ID, try view_mod.idKey(&kb, other, "send")));

    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    for ([_]NamespaceId{ .default, other }) |ns| {
        try registerIn(&t, ns, 1);
        try invokeIn(&t, ns, "r1", 10 * ms);
    }
    // Delivered in one, the other's run is still ready.
    try testing.expect((try deliverIn(&t, .default, "r1", 10 * ms, 20 * ms)).refused() == null);
    try testing.expectEqual(Status.leased, t.run(.default, "r1").?.status);
    try testing.expectEqual(Status.ready, t.run(other, "r1").?.status);
    // Deleted in one, the action is still the other's to invoke.
    try testing.expect((try t.apply(.action_delete, ae.Delete.Value{ .ns = DEFAULT, .name = "send" }, 30 * ms)).refused() == null);
    try testing.expectEqualStrings(NO_ACTION, (try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = "r2", .input = "", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, 40 * ms)).refused().?.refusal.message);
    try invokeIn(&t, other, "r2", 40 * ms);
    try testing.expect(t.run(.default, "r2") == null);
}

test "actions machine: an entry naming no namespace halts" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try testing.expectError(error.Halt, t.apply(.action_register, ae.Register.Value{ .ns = NamespaceId.invalid.int(), .name = "send", .owner = "", .lease_ms = LEASE_MS, .max_attempts = 1 }, 1));
}

test "actions machine: a lease below the floor, or past the ceiling, doesn't encode" {
    var buf: [ae.Deliver.max_size]u8 = undefined;
    const body: ae.Deliver.Value = .{ .ns = DEFAULT, .action = "send", .run = "r1", .due_ns = 0, .holder = "w1", .request_key = null, .lease_ms = 0 };
    try testing.expectError(error.OutOfRange, ae.Deliver.encodeInto(&buf, body));
    var long = body;
    long.lease_ms = ae.MAX_LEASE_MS + 1;
    try testing.expectError(error.OutOfRange, ae.Deliver.encodeInto(&buf, long));
}

test "actions machine: one log replayed onto another store leaves the same state" {
    var a = TestActions.init(testing.allocator);
    defer a.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&a, 2);
    try invoke(&a, "r1", 10 * ms);
    try invoke(&a, "r2", 11 * ms);
    _ = try deliver(&a, "r1", 10 * ms, 20 * ms);
    _ = try deliver(&a, "r2", 11 * ms, 21 * ms);
    _ = try a.apply(.action_extend, extendOf(&a, "r1"), 20 * ms + LEASE * 3 / 4);
    _ = try a.apply(.action_settle, settleOf(&a, "r1"), 30 * ms);
    const e2 = a.run(.default, "r2").?.expiry_ns;
    _ = try a.apply(.action_expire, try expiring(&ibuf, keyOf("r2", e2)), e2 + ms);
    _ = try deliver(&a, "r2", e2 + ms, e2 + 2 * ms);
    _ = try a.apply(.action_release, releaseOf(&a, "r2", false), e2 + 3 * ms);
    try testing.expectEqual(Status.failed, a.run(.default, "r2").?.status);

    var b = TestActions.init(testing.allocator);
    defer b.deinit();
    try a.replayInto(&b);
    try testing.expectEqual(a.sums(), b.sums());
}

test "actions machine: a report from a worker that doesn't hold the delivery is refused, and a second settle with another result is told the run has one" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const before = t.sums();
    var other = settleOf(&t, "r1");
    other.holder = "w2";
    try testing.expectEqualStrings("stale report: another worker holds this delivery", (try t.apply(.action_settle, other, 30 * ms)).refused().?.refusal.message);
    var ext = extendOf(&t, "r1");
    ext.holder = "w2";
    try testing.expect((try t.apply(.action_extend, ext, 30 * ms)).refused() != null);
    try testing.expectEqual(before, t.sums());

    const settle = settleOf(&t, "r1");
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    var second = settle;
    second.result = "other";
    try testing.expectEqualStrings(OTHER_RESULT, (try t.apply(.action_settle, second, 31 * ms)).refused().?.refusal.message);
    try testing.expectEqualStrings("done", t.run(.default, "r1").?.result);
}

test "actions machine: a delivery's request key is recorded at its deliver and goes when it settles, releases or expires" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try invoke(&t, "r2", 10 * ms);
    try invoke(&t, "r3", 10 * ms);
    for ([_][]const u8{ "r1", "r2", "r3" }, [_][]const u8{ "k1", "k2", "k3" }) |id, k| {
        const r = try t.apply(.action_deliver, ae.Deliver.Value{ .ns = DEFAULT, .action = "send", .run = id, .due_ns = 10 * ms, .holder = "w1", .request_key = k, .lease_ms = LEASE_MS }, 20 * ms);
        try testing.expect(r.refused() == null);
    }
    try testing.expectEqual(@as(usize, 3), t.count(Requests));
    var s = settleOf(&t, "r1");
    s.request_key = "k1";
    _ = try t.apply(.action_settle, s, 30 * ms);
    var rel = releaseOf(&t, "r2", false);
    rel.request_key = "k2";
    _ = try t.apply(.action_release, rel, 30 * ms);
    try testing.expectEqual(@as(usize, 1), t.count(Requests));
    const expiry = t.run(.default, "r3").?.expiry_ns;
    var item = keyOf("r3", expiry);
    item.request_key = "k3";
    _ = try t.apply(.action_expire, try expiring(&ibuf, item), expiry);
    try testing.expectEqual(@as(usize, 0), t.count(Requests));
}

test "actions machine: a run completed after a failed attempt keeps no error" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    var rel = releaseOf(&t, "r1", false);
    rel.delay_ms = 0;
    _ = try t.apply(.action_release, rel, 21 * ms);
    try testing.expectEqualStrings("boom", t.run(.default, "r1").?.@"error");
    try testing.expect((try deliver(&t, "r1", 21 * ms, 22 * ms)).refused() == null);
    _ = try t.apply(.action_settle, settleOf(&t, "r1"), 23 * ms);
    try testing.expectEqualStrings("", t.run(.default, "r1").?.@"error");
}

test "actions machine: every record's slot follows from what it stores, the action's name's" {
    const sc = @import("../contract/slot_check.zig");
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    // A tagged name: its slot is the tag's, which no name hash says.
    const action = "{acct:7}:send";
    const want = @import("../node/router.zig").nsKeySlot(.default, action);
    _ = try t.apply(.action_register, ae.Register.Value{ .ns = DEFAULT, .name = action, .owner = "", .lease_ms = LEASE_MS, .max_attempts = 3 }, 1);
    // One run left ready, one leased under a request key (a deadline and a
    // request record), and one chosen by a client.
    for ([_][]const u8{ "act-1a00-0", "act-2b00-0", "order-17" }, [_]u64{ 10, 11, 12 }) |run, at| {
        const r = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = action, .run = run, .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, at * ms);
        try testing.expect(r.refused() == null);
    }
    for ([_][]const u8{ "act-2b00-0", "order-17" }, [_]u64{ 11, 12 }) |run, due| {
        const r = try t.apply(.action_deliver, ae.Deliver.Value{ .ns = DEFAULT, .action = action, .run = run, .due_ns = due * ms, .holder = "w1", .request_key = run, .lease_ms = LEASE_MS }, 20 * ms);
        try testing.expect(r.refused() == null);
    }

    const D = struct {
        fn def(_: void, _: *store_mod_t, key: []const u8, _: []const u8) ?u32 {
            return @import("../node/router.zig").nsKeySlot(sc.nsOf(key), key[4..]);
        }
        fn run(_: void, _: *store_mod_t, key: []const u8, value: []const u8) ?u32 {
            const r = Runs.Value.decode(value) catch return null;
            return @import("../node/router.zig").nsKeySlot(sc.nsOf(key), r.v.action);
        }
        fn ready(_: void, _: *store_mod_t, key: []const u8, _: []const u8) ?u32 {
            return @import("../node/router.zig").nsKeySlot(sc.nsOf(key), sc.nameAfterNs(key));
        }
        fn deadline(_: void, _: *store_mod_t, key: []const u8, value: []const u8) ?u32 {
            // Past the family's empty namespace, as the duty reads it.
            const k = M.parseDeadlineKey(key[1..]) orelse return null;
            const d = Deadlines.Value.decode(value) catch return null;
            return @import("../node/router.zig").nsKeySlot(k.ns, d.v.scope);
        }
        // Keyed by holder: placed by its run's action, a join.
        fn request(_: void, store: *store_mod_t, key: []const u8, value: []const u8) ?u32 {
            const q = Requests.Value.decode(value) catch return null;
            var b: [4 + ae.MAX_RUN_ID]u8 = undefined;
            const rk = view_mod.idKey(&b, sc.nsOf(key), q.v.run) catch return null;
            return run({}, store, rk, store.get(Runs.ID, rk) orelse return null);
        }
    };
    try testing.expectEqual(@as(usize, 1), try sc.expectFamily(&t.store, Defs.ID, want, {}, D.def));
    try testing.expectEqual(@as(usize, 3), try sc.expectFamily(&t.store, Runs.ID, want, {}, D.run));
    try testing.expectEqual(@as(usize, 1), try sc.expectFamily(&t.store, Ready.ID, want, {}, D.ready));
    try testing.expectEqual(@as(usize, 2), try sc.expectFamily(&t.store, Deadlines.ID, want, {}, D.deadline));
    try testing.expectEqual(@as(usize, 2), try sc.expectFamily(&t.store, Requests.ID, want, {}, D.request));
}

const store_mod_t = @import("../contract/store.zig").Store;

test "actions machine: past its shard's share a settle still lands, its large result with it; a new run is refused" {
    const pool = @import("../budget/pool.zig");
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    // A share already full: nothing more fits.
    try settings.bindForTest(&t.store, pool.useOf(&t.store));
    const r2 = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = "r2", .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, 21 * ms);
    try testing.expectEqual(Reason.memory_budget, r2.refused().?.refusal.reason);
    // The run admitted is let finish, or its lease would expire and run it again.
    const big: [32 * 1024]u8 = @splat('r');
    var settle = settleOf(&t, "r1");
    settle.result = &big;
    const use = pool.useOf(&t.store);
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    try testing.expectEqual(Status.completed, t.run(.default, "r1").?.status);
    try testing.expect(pool.useOf(&t.store) > use);
}

test "actions machine: a run adds no more than its invoke's bytes, its input again in its history, and the call bound, every field at its largest" {
    const pool = @import("../budget/pool.zig");
    const memory_budget = @import("../node/memory_budget.zig");
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    const name: [ae.MAX_ACTION_NAME]u8 = @splat('a');
    try testing.expect((try t.apply(.action_register, ae.Register.Value{ .ns = DEFAULT, .name = &name, .owner = "", .lease_ms = LEASE_MS, .max_attempts = 3 }, 1)).refused() == null);
    const run: [ae.MAX_RUN_ID]u8 = @splat('r');
    const input: [ae.MAX_INPUT]u8 = @splat('i');
    const labels: [ae.MAX_LABELS]u8 = @splat('l');
    const caller: [limits.MAX_QUALIFIED_KEY]u8 = @splat('c');
    const inv: ae.Invoke.Value = .{ .ns = DEFAULT, .action = &name, .run = &run, .input = &input, .labels = &labels, .source = .direct, .caller_run = &caller, .caller_workflow = &caller };
    // Largest, its input goes to its history at once; a little under half
    // a chunk, it stays in the run's pending history.
    for ([_]usize{ ae.MAX_INPUT, hp.HIST_CHUNK / 2 - 64 }, 0..) |len, i| {
        var id = run;
        id[0] = @intCast('0' + i);
        var one = inv;
        one.run = &id;
        one.input = input[0..len];
        const before = pool.useOf(&t.store);
        try testing.expect((try t.apply(.action_invoke, one, 10 * ms)).refused() == null);
        const added = pool.useOf(&t.store) - before;
        try testing.expect(added <= try ae.Invoke.encodedSize(one) + len + memory_budget.CALL_RECORDS_BYTES);
    }
}

// ─── History ─────────────────────────────────────────────────────────────

/// The history chunks the group has sent itself, oldest first, as its
/// outbox holds them (copied into `a`).
fn histChunks(t: *TestActions, a: std.mem.Allocator) ![]he.Chunk.Value {
    var pb: [channels.CHAN_KEY]u8 = undefined;
    const prefix = channels.outPrefix(&pb, 0, .history);
    var it = (view_mod.ReadView{ .store = &t.store }).range(channels.OutFam, prefix);
    var got: std.ArrayListUnmanaged(he.Chunk.Value) = .empty;
    while (try it.next()) |item| {
        if (item.value.v.entry_type != @backingInt(EntryType.history_chunk)) continue;
        var diag: schema.Diagnostic = .{};
        try got.append(a, try he.Chunk.decodeAll(try a.dupe(u8, item.value.v.payload), &diag));
    }
    return got.items;
}

fn histDrops(t: *TestActions, a: std.mem.Allocator) ![]he.Drop.Value {
    var pb: [channels.CHAN_KEY]u8 = undefined;
    const prefix = channels.outPrefix(&pb, 0, .history);
    var it = (view_mod.ReadView{ .store = &t.store }).range(channels.OutFam, prefix);
    var got: std.ArrayListUnmanaged(he.Drop.Value) = .empty;
    while (try it.next()) |item| {
        if (item.value.v.entry_type != @backingInt(EntryType.history_drop)) continue;
        var diag: schema.Diagnostic = .{};
        try got.append(a, try he.Drop.decodeAll(try a.dupe(u8, item.value.v.payload), &diag));
    }
    return got.items;
}

test "actions machine: an action's counts move with its runs, once each, and its row goes when the last is deleted" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 2);
    const rv: view_mod.ReadView = .{ .store = &t.store };
    try invoke(&t, "r1", 10 * ms);
    try invoke(&t, "r2", 10 * ms);
    try testing.expectEqual(@as(u64, 2), countsOf(rv, .default, "send").ready);
    _ = try deliver(&t, "r1", 10 * ms, 20 * ms);
    try testing.expectEqual(@as(u64, 1), countsOf(rv, .default, "send").leased);
    // A deliver refused moves nothing.
    _ = try deliver(&t, "r1", 10 * ms, 21 * ms);
    try testing.expectEqual(@as(u64, 1), countsOf(rv, .default, "send").ready);
    // A release with attempts left: leased, then ready again.
    try testing.expect((try t.apply(.action_release, releaseOf(&t, "r1", false), 30 * ms)).refused() == null);
    try testing.expectEqual(@as(u64, 2), countsOf(rv, .default, "send").ready);
    try testing.expectEqual(@as(u64, 0), countsOf(rv, .default, "send").leased);
    _ = try deliver(&t, "r1", t.run(.default, "r1").?.due_ns, 40 * ms);
    var settle = settleOf(&t, "r1");
    try testing.expect((try t.apply(.action_settle, settle, 50 * ms)).refused() == null);
    // Its resend moves nothing.
    settle = settleOf(&t, "r1");
    _ = try t.apply(.action_settle, settle, 51 * ms);
    var c = countsOf(rv, .default, "send");
    try testing.expectEqual(@as(u64, 1), c.completed);
    try testing.expectEqual(@as(u64, 1), c.ready);
    try testing.expectEqual(@as(u64, 0), c.leased);
    // An expire that requeues, then one that ends it.
    _ = try deliver(&t, "r2", 10 * ms, 60 * ms);
    try t.duty(60 * ms + LEASE);
    try testing.expectEqual(@as(u64, 1), countsOf(rv, .default, "send").ready);
    _ = try deliver(&t, "r2", t.run(.default, "r2").?.due_ns, 60 * ms + LEASE + 1);
    try t.duty(60 * ms + 2 * LEASE + 2);
    c = countsOf(rv, .default, "send");
    try testing.expectEqual(@as(u64, 1), c.timed_out);
    try testing.expectEqual(@as(u64, 0), c.ready + c.leased);
    // Deletes take each end's count back; the row goes with the last.
    for ([_][]const u8{ "r1", "r2" }) |id| {
        const r = t.run(.default, id).?;
        try testing.expect((try t.apply(.action_run_delete, ae.RunDelete.Value{ .ns = DEFAULT, .action = "send", .run = id, .created_ns = r.created_ns }, 90 * ms)).refused() == null);
    }
    try testing.expectEqual(@as(usize, 0), t.count(Counts));
}

test "actions machine: a delete of an ended run removes it and drops its history; a live run, another incarnation or a resend is refused" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    const created = t.run(.default, "r1").?.created_ns;
    const del: ae.RunDelete.Value = .{ .ns = DEFAULT, .action = "send", .run = "r1", .created_ns = created };
    // Live: refused, and nothing changes.
    const live = try t.apply(.action_run_delete, del, 15 * ms);
    try testing.expectEqual(Reason.run_live, live.refused().?.refusal.reason);
    try testing.expect(t.run(.default, "r1") != null);
    _ = try deliver(&t, "r1", 10 * ms, 20 * ms);
    try testing.expect((try t.apply(.action_settle, settleOf(&t, "r1"), 30 * ms)).refused() == null);
    // Another incarnation, or another action's: not found.
    var other = del;
    other.created_ns += 1;
    try testing.expect((try t.apply(.action_run_delete, other, 35 * ms)).refused().?.refusal.reason == .not_found);
    other = del;
    other.action = "else";
    try testing.expect((try t.apply(.action_run_delete, other, 35 * ms)).refused().?.refusal.reason == .not_found);
    try testing.expectEqual(@as(usize, 0), (try histDrops(&t, a)).len);

    try testing.expect((try t.apply(.action_run_delete, del, 40 * ms)).refused() == null);
    try testing.expect(t.run(.default, "r1") == null);
    try testing.expectEqual(@as(usize, 0), t.count(Runs));
    const d = (try histDrops(&t, a))[0];
    try testing.expectEqualStrings("send", d.owner);
    try testing.expectEqualStrings("r1", d.run);
    try testing.expectEqual(created, d.created_ns);
    try testing.expectEqual(@as(u64, 30), d.completed_ms);
    // Resent: not found, nothing more sent. The id then names a new run.
    try testing.expect((try t.apply(.action_run_delete, del, 45 * ms)).refused().?.refusal.reason == .not_found);
    try testing.expectEqual(@as(usize, 1), (try histDrops(&t, a)).len);
    try invoke(&t, "r1", 50 * ms);
    try testing.expectEqual(Status.ready, t.run(.default, "r1").?.status);
}

/// The events `records` holds, in order.
fn histEvents(a: std.mem.Allocator, records: []const u8) ![]hp.HistRec.Value {
    var diag: schema.Diagnostic = .{};
    var r: schema.Reader = .{ .bytes = records, .diag = &diag };
    var got: std.ArrayListUnmanaged(hp.HistRec.Value) = .empty;
    while (r.pos < records.len) try got.append(a, try hp.HistRec.decode(&r, "rec"));
    return got.items;
}

/// Every event the group's chunks carry, in the order sent.
fn sentEvents(t: *TestActions, a: std.mem.Allocator) ![]hp.HistRec.Value {
    var all: std.ArrayListUnmanaged(hp.HistRec.Value) = .empty;
    for (try histChunks(t, a)) |c| try all.appendSlice(a, try histEvents(a, c.records));
    return all.items;
}

/// How many messages the group has sent, on either lane.
fn sendCount(t: *TestActions) u64 {
    const rv: view_mod.ReadView = .{ .store = &t.store };
    return channels.metaOf(rv, 0, .main).next_seq + channels.metaOf(rv, 0, .history).next_seq - 2;
}

fn eventNames(events: []const hp.HistRec.Value, out: [][]const u8) [][]const u8 {
    for (events, 0..) |e, i| out[i] = e.event;
    return out[0..events.len];
}

test "actions machine: a run's events go in its last chunk, sent as it ends, in order and at its seqs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    try testing.expect((try t.apply(.action_release, releaseOf(&t, "r1", false), 30 * ms)).refused() == null);
    try testing.expect((try deliver(&t, "r1", 35 * ms, 40 * ms)).refused() == null);
    // Nothing is sent before the end: each event is far under a chunk.
    try testing.expectEqual(@as(u64, 0), sendCount(&t));
    var settle = settleOf(&t, "r1");
    settle.outcome = "ok";
    try testing.expect((try t.apply(.action_settle, settle, 50 * ms)).refused() == null);

    const chunks = try histChunks(&t, a);
    try testing.expectEqual(@as(usize, 1), chunks.len);
    try testing.expect(chunks[0].final);
    try testing.expectEqual(@as(u64, 0), chunks[0].first_seq);
    try testing.expectEqual(@as(u64, 5), chunks[0].last_seq);
    try testing.expectEqual(@as(u64, 50), chunks[0].completed_ms);
    try testing.expectEqual(@backingInt(he.Family.action), chunks[0].family);
    try testing.expectEqualStrings("send", chunks[0].owner);
    const events = try histEvents(a, chunks[0].records);
    var nb: [8][]const u8 = undefined;
    const want = [_][]const u8{ EV_INVOKED, EV_DELIVERED, EV_RELEASED, EV_DELIVERED, EV_SETTLED, EV_ENDED };
    try testing.expectEqual(want.len, eventNames(events, &nb).len);
    for (want, events, 0..) |w, e, i| {
        try testing.expectEqualStrings(w, e.event);
        try testing.expectEqual(@as(u64, i), e.seq);
    }
    try testing.expectEqualStrings("in", events[0].detail);
    try testing.expectEqualStrings("w1", events[1].detail);
    try testing.expectEqualStrings("boom", events[2].detail);
    try testing.expectEqualStrings("done", events[4].detail);
    try testing.expectEqualStrings("ok", events[4].extra);
    try testing.expectEqualStrings("{\"abandons\":0,\"deliveries_unrecorded\":0}", events[5].detail);

    // Its row is its summary: no input, its result's preview and hash, and
    // nothing pending.
    const done = t.run(.default, "r1").?;
    try testing.expectEqualStrings("", done.input);
    try testing.expectEqualStrings("done", done.result);
    try testing.expect(!done.result_truncated);
    try testing.expectEqual(@import("stdx").hash.fnv1a64("done"), done.result_hash);
    try testing.expectEqual(@as(u64, 6), done.hist_next);
    try testing.expectEqual(@as(u64, 6), done.hist_flushed);
    try testing.expectEqualStrings("", done.hist_pending);
}

test "actions machine: an input over half a chunk is sent at its invoke, alone" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    const input: [hp.HIST_CHUNK / 2 + 1]u8 = @splat('i');
    const r = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = "r1", .input = &input, .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, 10 * ms);
    try testing.expect(r.refused() == null);
    const chunks = try histChunks(&t, arena.allocator());
    try testing.expectEqual(@as(usize, 1), chunks.len);
    try testing.expect(!chunks[0].final);
    try testing.expectEqual(@as(u64, 0), chunks[0].last_seq);
    const run = t.run(.default, "r1").?;
    try testing.expectEqual(@as(u64, 1), run.hist_flushed);
    try testing.expectEqualStrings("", run.hist_pending);
}

test "actions machine: deliveries past twice its attempts and hand-backs past its attempts are only counted, the counts said as it ends" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 1);
    try invoke(&t, "r1", 10 * ms);
    var due: u64 = 10 * ms;
    var stamp: u64 = 20 * ms;
    for (0..5) |_| {
        try testing.expect((try deliver(&t, "r1", due, stamp)).refused() == null);
        var rel = releaseOf(&t, "r1", true);
        rel.delay_ms = 0;
        try testing.expect((try t.apply(.action_release, rel, stamp + ms)).refused() == null);
        due = stamp + ms;
        stamp += 10 * ms;
    }
    try testing.expect((try deliver(&t, "r1", due, stamp)).refused() == null);
    try testing.expect((try t.apply(.action_settle, settleOf(&t, "r1"), stamp + ms)).refused() == null);
    const events = try sentEvents(&t, a);
    var delivered: usize = 0;
    var abandoned: usize = 0;
    for (events) |e| {
        if (std.mem.eql(u8, e.event, EV_DELIVERED)) delivered += 1;
        if (std.mem.eql(u8, e.event, EV_ABANDONED)) abandoned += 1;
    }
    try testing.expectEqual(@as(usize, 2), delivered);
    try testing.expectEqual(@as(usize, 1), abandoned);
    try testing.expectEqualStrings(EV_ENDED, events[events.len - 1].event);
    try testing.expectEqualStrings("{\"abandons\":5,\"deliveries_unrecorded\":4}", events[events.len - 1].detail);
}

test "actions machine: an expire of 24 runs, each with a pending chunk two-thirds full and a caller, applies within an apply's sends" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 1);
    // The largest input that waits in its chunk, and worker names and an
    // error cut to their previews.
    const input: [hp.HIST_CHUNK / 2 - 40]u8 = @splat('i');
    const holder: [1900]u8 = @splat('h');
    var ids: [ae.MAX_EXPIRE_ITEMS][8]u8 = undefined;
    for (&ids, 0..) |*id, i| {
        _ = try std.fmt.bufPrint(id, "r{d:0>7}", .{i});
        try testing.expect((try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = id, .input = &input, .labels = null, .source = .workflow, .caller_run = "wr", .caller_workflow = "wf" }, 10 * ms)).refused() == null);
        try testing.expect((try t.apply(.action_deliver, ae.Deliver.Value{ .ns = DEFAULT, .action = "send", .run = id, .due_ns = 10 * ms, .holder = &holder, .request_key = null, .lease_ms = LEASE_MS }, 11 * ms)).refused() == null);
        const r = t.run(.default, id).?;
        const back: ae.Release.Value = .{ .ns = DEFAULT, .action = "send", .run = id, .holder = &holder, .request_key = null, .deliveries = r.deliveries, .token = r.token, .expiry_ns = r.expiry_ns, .abandoned = true, .retry = true, .delay_ms = 0, .@"error" = &holder };
        try testing.expect((try t.apply(.action_release, back, 12 * ms)).refused() == null);
        try testing.expect((try t.apply(.action_deliver, ae.Deliver.Value{ .ns = DEFAULT, .action = "send", .run = id, .due_ns = 12 * ms, .holder = &holder, .request_key = null, .lease_ms = LEASE_MS }, 20 * ms)).refused() == null);
        try testing.expect(t.run(.default, id).?.hist_pending.len > hp.HIST_CHUNK * 2 / 3);
    }
    try testing.expectEqual(@as(u64, 0), sendCount(&t));
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(testing.allocator);
    for (&ids) |*id| {
        var ib: [ae.ExpireItem.max_size]u8 = undefined;
        try buf.appendSlice(testing.allocator, try ae.ExpireItem.encodeInto(&ib, .{ .ns = DEFAULT, .action = "send", .run = id, .expiry_ns = 20 * ms + LEASE, .holder = &holder, .request_key = null }));
    }
    const r = try t.apply(.action_expire, ae.Expire.Value{ .items = .{ .bytes = buf.items, .count = ae.MAX_EXPIRE_ITEMS } }, 20 * ms + LEASE);
    try testing.expect(r.refused() == null);
    try testing.expectEqual(@as(u64, 2 * ae.MAX_EXPIRE_ITEMS), sendCount(&t));
    for (&ids) |*id| try testing.expectEqual(Status.timed_out, t.run(.default, id).?.status);
}

test "actions machine: a 64 KiB result with a 256 B outcome lands whole in its history, its row a preview" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const result: [action_ops.MAX_OUTPUT]u8 = @splat('r');
    const outcome: [ae.MAX_OUTCOME]u8 = @splat('o');
    var settle = settleOf(&t, "r1");
    settle.result = &result;
    settle.outcome = &outcome;
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    const events = try sentEvents(&t, arena.allocator());
    const settled = events[events.len - 2];
    try testing.expectEqualStrings(EV_SETTLED, settled.event);
    try testing.expectEqualSlices(u8, &result, settled.detail);
    try testing.expectEqualSlices(u8, &outcome, settled.extra);
    const done = t.run(.default, "r1").?;
    try testing.expectEqual(@as(usize, PREVIEW), done.result.len);
    try testing.expect(done.result_truncated);
}

test "actions machine: a settle's resend is held to the first by its hash and sends nothing; another result is still refused" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try testing.expect((try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = "r1", .input = "in", .labels = null, .source = .workflow, .caller_run = "wr", .caller_workflow = "wf" }, 10 * ms)).refused() == null);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const result: [300]u8 = @splat('r');
    var settle = settleOf(&t, "r1");
    settle.result = &result;
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    const sums = t.sums();
    const sent = sendCount(&t);
    try testing.expect((try t.apply(.action_settle, settle, 31 * ms)).refused() == null);
    try testing.expectEqual(sums, t.sums());
    try testing.expectEqual(sent, sendCount(&t));
    var other = settle;
    var changed = result;
    changed[299] = 'x';
    other.result = &changed;
    const r = try t.apply(.action_settle, other, 32 * ms);
    try testing.expectEqualStrings(OTHER_RESULT, r.refused().?.refusal.message);
}

test "actions machine: a settle's result over its preview reaches its caller whole" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try testing.expect((try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = "r1", .input = "in", .labels = null, .source = .workflow, .caller_run = "wr", .caller_workflow = "wf" }, 10 * ms)).refused() == null);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const result: [PREVIEW + 44]u8 = @splat('r');
    var settle = settleOf(&t, "r1");
    settle.result = &result;
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    var pb: [channels.CHAN_KEY]u8 = undefined;
    var it = (view_mod.ReadView{ .store = &t.store }).range(channels.OutFam, channels.outPrefix(&pb, 0, .main));
    const out = (try it.next()).?.value.v;
    try testing.expectEqual(@backingInt(EntryType.call_done), out.entry_type);
    var diag: schema.Diagnostic = .{};
    const done = try ae.Done.decodeAll(out.payload, &diag);
    try testing.expectEqualSlices(u8, &result, done.result);
}

test "actions machine: a run's history is its action's, by a name with a brace and one at the longest" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const longest: [ae.MAX_ACTION_NAME]u8 = @splat('a');
    for ([_][]const u8{ "a}b", &longest }) |name| {
        var t = TestActions.init(testing.allocator);
        defer t.deinit();
        try testing.expect((try t.apply(.action_register, ae.Register.Value{ .ns = DEFAULT, .name = name, .owner = "", .lease_ms = LEASE_MS, .max_attempts = 3 }, 1)).refused() == null);
        try testing.expect((try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = name, .run = "r1", .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, 10 * ms)).refused() == null);
        try testing.expect((try t.apply(.action_deliver, ae.Deliver.Value{ .ns = DEFAULT, .action = name, .run = "r1", .due_ns = 10 * ms, .holder = "w1", .request_key = null, .lease_ms = LEASE_MS }, 20 * ms)).refused() == null);
        var settle = settleOf(&t, "r1");
        settle.action = name;
        try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
        const chunks = try histChunks(&t, arena.allocator());
        try testing.expectEqual(@as(usize, 1), chunks.len);
        try testing.expectEqualStrings(name, chunks[0].owner);
    }
}

test "actions machine: an attempt's worker and retried error are recorded as previews; the error that ends it, whole" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    const err: [action_ops.MAX_ERROR]u8 = @splat('e');
    const holder: [PREVIEW + 9]u8 = @splat('h');
    var due: u64 = 10 * ms;
    var stamp: u64 = 20 * ms;
    for (0..3) |i| {
        try testing.expect((try t.apply(.action_deliver, ae.Deliver.Value{ .ns = DEFAULT, .action = "send", .run = "r1", .due_ns = due, .holder = &holder, .request_key = null, .lease_ms = LEASE_MS }, stamp)).refused() == null);
        var rel = releaseOf(&t, "r1", false);
        rel.holder = &holder;
        rel.delay_ms = 0;
        rel.@"error" = &err;
        rel.retry = i < 2;
        try testing.expect((try t.apply(.action_release, rel, stamp + ms)).refused() == null);
        due = stamp + ms;
        stamp += 10 * ms;
    }
    try testing.expectEqual(Status.failed, t.run(.default, "r1").?.status);
    const events = try sentEvents(&t, arena.allocator());
    // invoked, then delivered and released three times, then ended.
    try testing.expectEqual(@as(usize, 8), events.len);
    for (events[1..7], 0..) |e, i| {
        const whole = i == 5;
        try testing.expectEqualStrings(if (i % 2 == 0) EV_DELIVERED else EV_RELEASED, e.event);
        try testing.expectEqual(@as(usize, if (whole) action_ops.MAX_ERROR else PREVIEW), e.detail.len);
        try testing.expectEqualStrings(if (whole) "" else TRUNCATED, e.extra);
    }
    try testing.expectEqualStrings(EV_ENDED, events[7].event);
}

/// `body`, an invoke, applied as a call's start: a message on the main lane
/// from the calling namespace.
/// Calls' starts, all in one inbound, in order.
fn applyCalls(t: *TestActions, bodies: []const ae.Invoke.Value, stamp: u64) !Result {
    const ce = @import("../protocol/channel_entries.zig");
    const a = t.scratch.child_allocator;
    var mw: std.Io.Writer.Allocating = .init(a);
    defer mw.deinit();
    for (bodies) |body| {
        const payload = try a.alloc(u8, try ae.Invoke.encodedSize(body));
        defer a.free(payload);
        _ = try ae.Invoke.encodeInto(payload, body);
        const mb = try a.alloc(u8, payload.len + 64);
        defer a.free(mb);
        var w: schema.Writer = .{ .buf = mb };
        try ce.Message.encode(&w, .{ .slot = @intCast(router.nsKeySlot(.of(body.ns), body.action)), .entry_type = @backingInt(EntryType.action_invoke), .layout_epoch = 0, .flags = 0, .origin_kind = @backingInt(view_mod.Origin.Kind.user), .origin_ns = body.ns, .payload = payload });
        try mw.writer.writeAll(w.written());
    }
    const rv: view_mod.ReadView = .{ .store = &t.store };
    const seq = (if (channels.inOf(rv, 0, .main)) |i| i.applied else 0) + 1;
    const ib = try a.alloc(u8, mw.written().len + 64);
    defer a.free(ib);
    const inbound = try ce.Inbound.encodeInto(ib, .{ .from = 0, .lane = @backingInt(channels.Lane.main), .first_seq = seq, .messages = .{ .bytes = mw.written(), .count = @intCast(bodies.len) } });
    t.index += 1;
    const e = entry_mod.buildEntry(.channel_inbound, entry_mod.Flags.NONE, t.term, t.index, stamp, inbound);
    var out: Result = .{};
    out.reset(&e);
    try machine_mod.applyEntry(&channels.Channels(&.{machine}).machine, &t.store, &t.scratch, &e, &out);
    return out;
}

test "actions machine: a call over both its namespace's quota and the shard's memory share waits on the quota; an invoke's verdict counts the workflow runs' reserve" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try hq.setForTest(&t.store, .default, 1 << 30, 1 << 30);
    try settings.bindForTest(&t.store, 1);
    const call: ae.Invoke.Value = .{ .ns = DEFAULT, .action = "send", .run = "c0", .input = "", .labels = null, .source = .workflow, .caller_run = "wr", .caller_workflow = "wf" };
    try testing.expect((try applyCall(&t, call, 10 * ms)).refused() == null);
    try testing.expectEqual(ae.Answer.over_quota, (try lastReply(&t, arena.allocator())).answer);
    // Room for one run, the workflow runs reserving most of it: refused.
    try settings.bindForTest(&t.store, std.math.maxInt(u64));
    const each = history_reserve.action(4, 2, 3, 0, 0, 0, false);
    try hq.setForTest(&t.store, .default, each + 100, 0);
    try hq.setReservedForTest(hq.WorkflowReserved, &t.store, .default, 200);
    const direct: ae.Invoke.Value = .{ .ns = DEFAULT, .action = "send", .run = "d0", .input = "", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null };
    try testing.expectEqual(Reason.history_quota, (try t.apply(.action_invoke, direct, 20 * ms)).refused().?.refusal.reason);
    try hq.setReservedForTest(hq.WorkflowReserved, &t.store, .default, 0);
    try testing.expect((try t.apply(.action_invoke, direct, 21 * ms)).refused() == null);
}

test "actions machine: at its namespace's quota a direct invoke is refused history_quota and a call's answered over_quota, raising nothing; calls in one inbound are judged one after another" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    const read: view_mod.ReadView = .{ .store = &t.store };
    try register(&t, 3);
    const each = history_reserve.action(4, 2, 3, 0, 0, 0, false);
    // Room for three runs' reserves exactly.
    try hq.setForTest(&t.store, .default, 3 * each, 0);
    const Call = struct {
        fn of(run_id: []const u8) ae.Invoke.Value {
            return .{ .ns = DEFAULT, .action = "send", .run = run_id, .input = "", .labels = null, .source = .workflow, .caller_run = "wr", .caller_workflow = "wf" };
        }
    };
    const batch = [_]ae.Invoke.Value{ Call.of("c0"), Call.of("c1"), Call.of("c2"), Call.of("c3"), Call.of("c4") };
    try testing.expect((try applyCalls(&t, &batch, 10 * ms)).refused() == null);
    var started: usize = 0;
    var held: usize = 0;
    var pb: [channels.CHAN_KEY]u8 = undefined;
    var it = read.range(channels.OutFam, channels.outPrefix(&pb, 0, .main));
    while (try it.next()) |item| {
        if (item.value.v.entry_type != @backingInt(EntryType.call_reply)) continue;
        var diag: schema.Diagnostic = .{};
        const r = try ae.Reply.decodeAll(try a.dupe(u8, item.value.v.payload), &diag);
        switch (r.answer) {
            .started => started += 1,
            .over_quota => {
                held += 1;
                try testing.expect(std.mem.startsWith(u8, r.why.?, "limit: ns 1 history 0+"));
                try testing.expect(t.run(.default, r.run) == null);
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(usize, 3), started);
    try testing.expectEqual(@as(usize, 2), held);
    try testing.expectEqual(3 * each, hq.reservedOf(hq.ActionReserved, read, .default));
    // A direct invoke past it: refused whole, nothing written.
    const before = t.sums();
    var direct = Call.of("d0");
    direct.source = .direct;
    direct.caller_run = null;
    direct.caller_workflow = null;
    const rf = (try t.apply(.action_invoke, direct, 20 * ms)).refused().?;
    try testing.expectEqual(Reason.history_quota, rf.refusal.reason);
    try testing.expectEqual(before, t.sums());
    try testing.expectEqual(3 * each, hq.reservedOf(hq.ActionReserved, read, .default));
    // A run whose own reserve is over the part is told so.
    try hq.setForTest(&t.store, .default, each - 1, 0);
    const own = (try t.apply(.action_invoke, direct, 21 * ms)).refused().?;
    try testing.expect(std.mem.startsWith(u8, own.refusal.message, "limit: ns 1 run reserve "));
}

fn applyCall(t: *TestActions, body: ae.Invoke.Value, stamp: u64) !Result {
    const ce = @import("../protocol/channel_entries.zig");
    const a = t.scratch.child_allocator;
    const payload = try a.alloc(u8, try ae.Invoke.encodedSize(body));
    defer a.free(payload);
    _ = try ae.Invoke.encodeInto(payload, body);
    const mb = try a.alloc(u8, payload.len + 64);
    defer a.free(mb);
    var w: schema.Writer = .{ .buf = mb };
    try ce.Message.encode(&w, .{ .slot = @intCast(router.nsKeySlot(.of(body.ns), body.action)), .entry_type = @backingInt(EntryType.action_invoke), .layout_epoch = 0, .flags = 0, .origin_kind = @backingInt(view_mod.Origin.Kind.user), .origin_ns = body.ns, .payload = payload });
    const rv: view_mod.ReadView = .{ .store = &t.store };
    const seq = (if (channels.inOf(rv, 0, .main)) |i| i.applied else 0) + 1;
    const ib = try a.alloc(u8, w.written().len + 64);
    defer a.free(ib);
    const inbound = try ce.Inbound.encodeInto(ib, .{ .from = 0, .lane = @backingInt(channels.Lane.main), .first_seq = seq, .messages = .{ .bytes = w.written(), .count = 1 } });
    t.index += 1;
    const e = entry_mod.buildEntry(.channel_inbound, entry_mod.Flags.NONE, t.term, t.index, stamp, inbound);
    var out: Result = .{};
    out.reset(&e);
    try machine_mod.applyEntry(&channels.Channels(&.{machine}).machine, &t.store, &t.scratch, &e, &out);
    return out;
}

/// The answer the group last sent to a call's start.
fn lastReply(t: *TestActions, a: std.mem.Allocator) !ae.Reply.Value {
    var pb: [channels.CHAN_KEY]u8 = undefined;
    var it = (view_mod.ReadView{ .store = &t.store }).range(channels.OutFam, channels.outPrefix(&pb, 0, .main));
    var last: ?ae.Reply.Value = null;
    while (try it.next()) |item| {
        if (item.value.v.entry_type != @backingInt(EntryType.call_reply)) continue;
        var diag: schema.Diagnostic = .{};
        last = try ae.Reply.decodeAll(try a.dupe(u8, item.value.v.payload), &diag);
    }
    return last orelse error.TestNoReply;
}

test "actions machine: a call past its namespace's share of the history lane is answered over_budget, creating nothing; another namespace's call starts" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    const busy: NamespaceId = .of(7);
    const quiet: NamespaceId = .of(8);
    try registerIn(&t, busy, 3);
    try registerIn(&t, quiet, 3);
    // A cap of 16 KB, a quarter of the bound share: past it, a namespace
    // holding three quarters of it is held.
    try settings.bindForTest(&t.store, 64_000);
    const input: [3000]u8 = @splat('i');
    const Call = struct {
        fn of(ns: NamespaceId, run_id: []const u8, in: []const u8) ae.Invoke.Value {
            return .{ .ns = ns.int(), .action = "send", .run = run_id, .input = in, .labels = null, .source = .workflow, .caller_run = "wr", .caller_workflow = "wf" };
        }
    };
    // Each input is over half a chunk, so goes to the lane as it starts.
    for ([_][]const u8{ "b1", "b2", "b3", "b4", "b5", "b6" }) |id| {
        try testing.expect((try applyCall(&t, Call.of(busy, id, &input), 10 * ms)).refused() == null);
        try testing.expectEqual(ae.Answer.started, (try lastReply(&t, a)).answer);
    }
    try testing.expect((try applyCall(&t, Call.of(busy, "b7", &input), 10 * ms)).refused() == null);
    const held = try lastReply(&t, a);
    try testing.expectEqual(ae.Answer.over_budget, held.answer);
    try testing.expectEqualStrings("b7", held.run);
    try testing.expect(std.mem.indexOf(u8, held.why.?, "history lane") != null);
    try testing.expect(t.run(busy, "b7") == null);
    // Past the lane and at its quota too: it waits on the quota, which a
    // call that can't wait still does, rather than failing on the lane.
    try hq.setForTest(&t.store, busy, 1 << 30, 1 << 30);
    try testing.expect((try applyCall(&t, Call.of(busy, "b8", &input), 10 * ms)).refused() == null);
    const both = try lastReply(&t, a);
    try testing.expectEqual(ae.Answer.over_quota, both.answer);
    try testing.expect(std.mem.startsWith(u8, both.why.?, "limit: ns 7 history "));
    try testing.expect(t.run(busy, "b8") == null);
    try hq.setForTest(&t.store, busy, settings.NO_QUOTA, 0);
    // The namespace holding none of the lane is not held by it.
    try testing.expect((try applyCall(&t, Call.of(quiet, "q1", &input), 10 * ms)).refused() == null);
    try testing.expectEqual(ae.Answer.started, (try lastReply(&t, a)).answer);
    try testing.expect(t.run(quiet, "q1") != null);
}
