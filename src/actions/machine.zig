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
const ae = @import("../protocol/action_entries.zig");
const action_ops = @import("../protocol/action_ops.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");

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

/// A family value stored as a schema record, so its codec is the schema's.
fn Stored(comptime R: type) type {
    return struct {
        v: R.Value,

        pub fn encode(self: @This(), w: *std.Io.Writer) std.Io.Writer.Error!void {
            const n = R.encodedSize(self.v) catch return error.WriteFailed;
            _ = R.encodeInto(try w.writableSlice(n), self.v) catch return error.WriteFailed;
        }

        pub fn decode(bytes: []const u8) error{Malformed}!@This() {
            var diag: schema.Diagnostic = .{};
            return .{ .v = R.decodeAll(bytes, &diag) catch return error.Malformed };
        }
    };
}

pub const Def = Record(&.{
    .{ .name = "owner", .T = Bytes(ae.MAX_OWNER) },
    .{ .name = "version", .T = Int(u32) },
    .{ .name = "lease_ms", .T = Int(u64) },
    .{ .name = "max_attempts", .T = Int(u32) },
    .{ .name = "created_ns", .T = Int(u64) },
});

pub const Status = enum(u8) { ready = 0, leased = 1, completed = 2, failed = 3, timed_out = 4 };

/// How the run's latest delivery ended, so a resent report is answered by
/// what happened to it.
pub const Ended = enum(u8) { none = 0, settled = 1, released = 2, abandoned = 3, expired = 4 };

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
    .{ .name = "result", .T = Bytes(action_ops.MAX_OUTPUT) },
    .{ .name = "error", .T = Bytes(action_ops.MAX_ERROR) },
});

/// The action a deadline's run belongs to, so the duty can name its ready
/// key; the namespace and run are in the deadline key.
pub const Deadline = Record(&.{
    .{ .name = "action", .T = ae.ActionName },
});

pub const Empty = struct {
    pub fn encode(_: Empty, _: *std.Io.Writer) std.Io.Writer.Error!void {}
    pub fn decode(bytes: []const u8) error{Malformed}!Empty {
        if (bytes.len != 0) return error.Malformed;
        return .{};
    }
};

pub const Defs = view_mod.Family(20, "action.def", Stored(Def));
pub const Runs = view_mod.Family(21, "action.run", Stored(Run));
pub const Ready = view_mod.Family(22, "action.ready", Empty);
pub const Deadlines = view_mod.Family(23, "action.deadline", Stored(Deadline));

// ─── Keys ────────────────────────────────────────────────────────────────

/// The longest key a family here holds, before its namespace prefix.
const MAX_KEY = ae.MAX_ACTION_NAME + 1 + 8 + ae.MAX_RUN_ID;

const KeyBuf = [MAX_KEY]u8;

comptime {
    std.debug.assert(8 + 1 + limits.MAX_NAMESPACE_NAME + ae.MAX_RUN_ID <= MAX_KEY);
}

/// `[action][0][due:u64 BE][run]`: an action's ready runs, earliest first.
/// A name holds no NUL (the register refuses one), so one action's keys
/// never run into another's.
pub fn readyKey(buf: []u8, action: []const u8, due_ns: u64, run: []const u8) []const u8 {
    @memcpy(buf[0..action.len], action);
    buf[action.len] = 0;
    std.mem.writeInt(u64, buf[action.len + 1 ..][0..8], due_ns, .big);
    @memcpy(buf[action.len + 9 ..][0..run.len], run);
    return buf[0 .. action.len + 9 + run.len];
}

/// The prefix of an action's ready keys: `[action][0]`.
pub fn readyPrefix(buf: []u8, action: []const u8) []const u8 {
    @memcpy(buf[0..action.len], action);
    buf[action.len] = 0;
    return buf[0 .. action.len + 1];
}

/// `[expiry:u64 BE][ns len:u8][ns][run]`, in no namespace: every lease,
/// earliest first. The namespace is in it, so the same run id in two
/// namespaces is two keys.
pub fn deadlineKey(buf: []u8, expiry_ns: u64, ns: []const u8, run: []const u8) []const u8 {
    std.mem.writeInt(u64, buf[0..8], expiry_ns, .big);
    buf[8] = @intCast(ns.len);
    @memcpy(buf[9..][0..ns.len], ns);
    @memcpy(buf[9 + ns.len ..][0..run.len], run);
    return buf[0 .. 9 + ns.len + run.len];
}

pub const DeadlineKey = struct { expiry_ns: u64, ns: []const u8, run: []const u8 };

/// A deadline key read back, without its family's namespace prefix.
pub fn parseDeadlineKey(k: []const u8) ?DeadlineKey {
    if (k.len < 9 or k.len < 9 + @as(usize, k[8])) return null;
    const ns_len = k[8];
    return .{ .expiry_ns = std.mem.readInt(u64, k[0..8], .big), .ns = k[9..][0..ns_len], .run = k[9 + ns_len ..] };
}

fn after(stamp_ns: u64, millis: u64) u64 {
    return stamp_ns +| (millis *| std.time.ns_per_ms);
}

/// Where an extend at `stamp` moves a lease to: a lease on from now, but
/// never past the hold ceiling from its deliver.
fn extendedTo(stamp: u64, lease_ms: u64, held_since_ns: u64) u64 {
    return @min(after(stamp, lease_ms), after(held_since_ns, ae.MAX_HOLD_MS));
}

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    var b: KeyBuf = undefined;
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .action_register => {
            const p = ae.Register.decodeAll(e.payload, &diag) catch return;
            try out.add(Defs.ID, p.ns, p.name);
        },
        .action_delete => {
            const p = ae.Delete.decodeAll(e.payload, &diag) catch return;
            try out.add(Defs.ID, p.ns, p.name);
        },
        .action_invoke => {
            const p = ae.Invoke.decodeAll(e.payload, &diag) catch return;
            try out.add(Defs.ID, p.ns, p.action);
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Ready.ID, p.ns, readyKey(&b, p.action, stamp, p.run));
        },
        .action_deliver => {
            const p = ae.Deliver.decodeAll(e.payload, &diag) catch return;
            try out.add(Defs.ID, p.ns, p.action);
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Ready.ID, p.ns, readyKey(&b, p.action, p.due_ns, p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, after(stamp, p.lease_ms), p.ns, p.run));
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return;
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, p.ns, p.run));
        },
        .action_release => {
            const p = ae.Release.decodeAll(e.payload, &diag) catch return;
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Ready.ID, p.ns, readyKey(&b, p.action, after(stamp, p.delay_ms), p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, p.ns, p.run));
        },
        .action_extend => {
            const p = ae.Extend.decodeAll(e.payload, &diag) catch return;
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, p.ns, p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, extendedTo(stamp, p.lease_ms, p.held_since_ns), p.ns, p.run));
        },
        .action_expire => {
            const p = ae.Expire.decodeAll(e.payload, &diag) catch return;
            var it = p.items.iterator();
            while (it.next() catch return) |item| {
                try out.add(Runs.ID, item.ns, item.run);
                try out.add(Deadlines.ID, "", deadlineKey(&b, item.expiry_ns, item.ns, item.run));
                try out.add(Ready.ID, item.ns, readyKey(&b, item.action, stamp, item.run));
            }
        },
        else => {},
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────

pub const NO_ACTION = "not found: no such action";
pub const NO_RUN = "not found: no such run";
pub const RUN_EXISTS = "conflict: a run with this id exists";
pub const NOT_READY = "conflict: the run isn't ready to deliver";
pub const NAME_HAS_NUL = "bad request: an action's name holds no NUL";
pub const NOT_ITS_LEASE = "bad request: an extend names its delivery's own lease and start";

/// Answers: deliver, the run's delivery count (u32), which with the
/// entry's (term, index) is the delivery id; expire, the leases it ended
/// (u32). The rest answer nothing.
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .action_register => {
            const p = ae.Register.decodeAll(e.payload, &diag) catch return error.Halt;
            if (p.ns.len == 0) return error.Halt;
            if (std.mem.indexOfScalar(u8, p.name, 0) != null) return out.refuse(.bad_request, .malformed, NAME_HAS_NUL);
            const k = try nsKey(view, p.ns, p.name);
            const created = if (try view.get(Defs, k)) |d| d.v.created_ns else stamp;
            try view.put(Defs, k, .{ .v = .{ .owner = p.owner, .version = p.version, .lease_ms = p.lease_ms, .max_attempts = p.max_attempts, .created_ns = created } });
        },
        .action_delete => {
            const p = ae.Delete.decodeAll(e.payload, &diag) catch return error.Halt;
            const k = try nsKey(view, p.ns, p.name);
            if (try view.get(Defs, k) == null) return out.refuse(.not_found, .not_found, NO_ACTION);
            try view.delete(Defs, k);
        },
        .action_invoke => {
            const p = ae.Invoke.decodeAll(e.payload, &diag) catch return error.Halt;
            const def = (try view.get(Defs, try nsKey(view, p.ns, p.action))) orelse return out.refuse(.not_found, .not_found, NO_ACTION);
            const rk = try nsKey(view, p.ns, p.run);
            if (try view.get(Runs, rk) != null) return out.refuse(.conflict, .unclassified, RUN_EXISTS);
            try view.put(Runs, rk, .{ .v = .{
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
                .token = .{ .term = 0, .index = 0 },
                .lease_ms = 0,
                .delivered_ns = 0,
                .expiry_ns = 0,
                .ended = .none,
                .started_ns = 0,
                .completed_ns = 0,
                .outcome = "",
                .result = "",
                .@"error" = "",
            } });
            var b: KeyBuf = undefined;
            try view.put(Ready, try nsKey(view, p.ns, readyKey(&b, p.action, stamp, p.run)), .{});
        },
        .action_deliver => {
            const p = ae.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
            if (try view.get(Defs, try nsKey(view, p.ns, p.action)) == null) return out.refuse(.not_found, .not_found, NO_ACTION);
            const rk = try nsKey(view, p.ns, p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            // The ready key the leader chose by must still be the run's,
            // and due by the log's clock.
            if (run.v.status != .ready or run.v.due_ns != p.due_ns or p.due_ns > stamp)
                return out.refuse(.conflict, .unclassified, NOT_READY);
            var b: KeyBuf = undefined;
            try view.delete(Ready, try nsKey(view, p.ns, readyKey(&b, p.action, p.due_ns, p.run)));
            run.v.status = .leased;
            run.v.deliveries +|= 1;
            run.v.holder = p.holder;
            run.v.token = .{ .term = e.header.term, .index = e.header.index };
            run.v.lease_ms = p.lease_ms;
            run.v.delivered_ns = stamp;
            run.v.expiry_ns = after(stamp, p.lease_ms);
            run.v.ended = .none;
            if (run.v.started_ns == 0) run.v.started_ns = stamp;
            try view.put(Runs, rk, run);
            try putDeadline(view, p.ns, p.run, run.v);
            out.putInt(u32, run.v.deliveries);
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try nsKey(view, p.ns, p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.deliveries, p.token)) {
                // A resend of the settle that applied answers as it did.
                if (sameDelivery(run.v, p.deliveries, p.token) and run.v.ended == .settled) return;
                return stale(out, run.v, p.deliveries, p.token);
            }
            try dropDeadline(view, p.ns, p.run, run.v, p.expiry_ns);
            run.v.status = .completed;
            run.v.ended = .settled;
            run.v.outcome = p.outcome;
            run.v.result = p.result;
            run.v.completed_ns = stamp;
            try view.put(Runs, rk, run);
        },
        .action_release => {
            const p = ae.Release.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try nsKey(view, p.ns, p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.deliveries, p.token)) {
                const ended: Ended = if (p.abandoned) .abandoned else .released;
                if (sameDelivery(run.v, p.deliveries, p.token) and run.v.ended == ended) return;
                return stale(out, run.v, p.deliveries, p.token);
            }
            try dropDeadline(view, p.ns, p.run, run.v, p.expiry_ns);
            run.v.@"error" = p.@"error";
            if (p.abandoned) {
                run.v.ended = .abandoned;
                try readyAgain(view, p.ns, p.run, &run, after(stamp, p.delay_ms));
            } else {
                run.v.ended = .released;
                run.v.failures +|= 1;
                if (!p.retry or run.v.failures >= run.v.max_attempts) {
                    run.v.status = .failed;
                    run.v.completed_ns = stamp;
                } else try readyAgain(view, p.ns, p.run, &run, after(stamp, p.delay_ms));
            }
            try view.put(Runs, rk, run);
        },
        .action_extend => {
            const p = ae.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try nsKey(view, p.ns, p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, run.v.deliveries, p.token)) return stale(out, run.v, run.v.deliveries, p.token);
            if (p.lease_ms != run.v.lease_ms or p.held_since_ns != run.v.delivered_ns) return out.refuse(.bad_request, .unclassified, NOT_ITS_LEASE);
            // More than half the lease left, or held to the ceiling: the
            // lease stands, and nothing is written.
            if (run.v.expiry_ns -| stamp > (run.v.lease_ms *| std.time.ns_per_ms) / 2) return;
            const to = extendedTo(stamp, p.lease_ms, p.held_since_ns);
            if (to <= run.v.expiry_ns) return;
            try dropDeadline(view, p.ns, p.run, run.v, p.expiry_ns);
            run.v.expiry_ns = to;
            try view.put(Runs, rk, run);
            try putDeadline(view, p.ns, p.run, run.v);
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
    const rk = try nsKey(view, item.ns, item.run);
    const found = try view.get(Runs, rk);
    const live = if (found) |r| r.v.status == .leased and r.v.expiry_ns == item.expiry_ns else false;
    const ours = if (found) |r| std.mem.eql(u8, r.v.action, item.action) else true;
    if (!ours and live) return false;
    var b: KeyBuf = undefined;
    try view.delete(Deadlines, try nsKey(view, "", deadlineKey(&b, item.expiry_ns, item.ns, item.run)));
    if (!live) return false;
    var run = found.?;
    run.v.ended = .expired;
    run.v.failures +|= 1;
    run.v.@"error" = "lease expired";
    if (run.v.failures >= run.v.max_attempts) {
        run.v.status = .timed_out;
        run.v.completed_ns = stamp;
    } else try readyAgain(view, item.ns, item.run, &run, stamp);
    try view.put(Runs, rk, run);
    return true;
}

/// The run at `rk`, if it is `action`'s.
fn inScope(view: *View, rk: []const u8, action: []const u8) ApplyError!?Stored(Run) {
    const run = (try view.get(Runs, rk)) orelse return null;
    return if (std.mem.eql(u8, run.v.action, action)) run else null;
}

fn readyAgain(view: *View, ns: []const u8, run_id: []const u8, run: *Stored(Run), due_ns: u64) ApplyError!void {
    run.v.status = .ready;
    run.v.due_ns = due_ns;
    var b: KeyBuf = undefined;
    try view.put(Ready, try nsKey(view, ns, readyKey(&b, run.v.action, due_ns, run_id)), .{});
}

fn putDeadline(view: *View, ns: []const u8, run_id: []const u8, run: Run.Value) ApplyError!void {
    // An extend or a redeliver landing on the same instant rewrites the
    // same key: still one deadline for the one live lease.
    var b: KeyBuf = undefined;
    try view.put(Deadlines, try nsKey(view, "", deadlineKey(&b, run.expiry_ns, ns, run_id)), .{ .v = .{ .action = run.action } });
}

/// Delete the key at `seen`, the expiry the leader saw, if it is still the
/// run's; if an extend moved it since, the duty drops it once past due.
fn dropDeadline(view: *View, ns: []const u8, run_id: []const u8, run: Run.Value, seen: u64) ApplyError!void {
    if (seen != run.expiry_ns) return;
    var b: KeyBuf = undefined;
    try view.delete(Deadlines, try nsKey(view, "", deadlineKey(&b, seen, ns, run_id)));
}

/// Whether the report's delivery is the run's live one.
fn holds(run: Run.Value, deliveries: u32, token: ae.Token.Value) bool {
    return run.status == .leased and sameDelivery(run, deliveries, token);
}

fn sameDelivery(run: Run.Value, deliveries: u32, token: ae.Token.Value) bool {
    return run.deliveries == deliveries and run.token.term == token.term and run.token.index == token.index;
}

/// A report for a delivery that has moved on, said by what happened.
fn stale(out: *Result, run: Run.Value, deliveries: u32, token: ae.Token.Value) void {
    const why = if (!sameDelivery(run, deliveries, token))
        "stale report: the run was delivered again since"
    else switch (run.ended) {
        .expired => "stale report: the lease expired and the run moved on",
        .settled => "stale report: the run was completed",
        .released, .abandoned => "stale report: the run was released",
        .none => "stale report: the run isn't held",
    };
    out.refuse(.conflict, .stale_report, why);
}

/// `k` in `ns`, as a family stores it, in the entry's scratch.
fn nsKey(view: *View, ns: []const u8, k: []const u8) ApplyError![]const u8 {
    if (ns.len > view_mod.MAX_NAMESPACE) return error.Halt;
    const buf = try view.arena.alloc(u8, 1 + ns.len + k.len);
    return view_mod.key(buf, ns, k) catch unreachable;
}

pub const machine: machine_mod.Machine = .{
    .name = "actions",
    // DRAFT: register, delete and invoke join once the handler stops
    // applying them; until then the shard's one-applier check gives them
    // to the handler. `apply` already takes all eight.
    .entry_types = &.{ .action_deliver, .action_settle, .action_release, .action_extend, .action_expire },
    .families = &.{ machine_mod.info(Defs), machine_mod.info(Runs), machine_mod.info(Ready), machine_mod.info(Deadlines) },
    .keys = keys,
    .apply = apply,
};

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

    pub fn run(self: *TestActions, ns: []const u8, id: []const u8) ?Run.Value {
        var b: [1 + limits.MAX_NAMESPACE_NAME + ae.MAX_RUN_ID]u8 = undefined;
        const k = view_mod.key(&b, ns, id) catch unreachable;
        const got = (view_mod.ReadView{ .store = &self.store }).get(Runs, k) catch unreachable;
        return if (got) |r| r.v else null;
    }

    /// Every family's checksum: equal before and after means nothing changed.
    pub fn sums(self: *TestActions) [4]u64 {
        var out: [4]u64 = undefined;
        inline for (.{ Defs, Runs, Ready, Deadlines }, 0..) |F, i| out[i] = store_mod.checksum(&self.store, F.ID);
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
            try buf.appendSlice(self.store.allocator, try ae.ExpireItem.encodeInto(&ib, .{ .ns = k.ns, .action = d.v.action, .run = k.run, .expiry_ns = k.expiry_ns }));
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

fn registerIn(t: *TestActions, ns: []const u8, max_attempts: u32) !void {
    const r = try t.apply(.action_register, ae.Register.Value{ .ns = ns, .name = "send", .owner = "", .version = 1, .lease_ms = LEASE_MS, .max_attempts = max_attempts }, 1);
    try testing.expect(r.refused() == null);
}

fn register(t: *TestActions, max_attempts: u32) !void {
    return registerIn(t, "default", max_attempts);
}

fn invokeIn(t: *TestActions, ns: []const u8, run_id: []const u8, stamp: u64) !void {
    const r = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = ns, .action = "send", .run = run_id, .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, stamp);
    try testing.expect(r.refused() == null);
}

fn invoke(t: *TestActions, run_id: []const u8, stamp: u64) !void {
    return invokeIn(t, "default", run_id, stamp);
}

fn deliverIn(t: *TestActions, ns: []const u8, run_id: []const u8, due: u64, stamp: u64) !Result {
    return t.apply(.action_deliver, ae.Deliver.Value{ .ns = ns, .action = "send", .run = run_id, .due_ns = due, .holder = "w1", .request_key = null, .lease_ms = LEASE_MS }, stamp);
}

fn deliver(t: *TestActions, run_id: []const u8, due: u64, stamp: u64) !Result {
    return deliverIn(t, "default", run_id, due, stamp);
}

/// A settle of the run's live delivery, as its leader would propose it.
fn settleOf(t: *TestActions, run_id: []const u8) ae.Settle.Value {
    const r = t.run("default", run_id).?;
    return .{ .ns = "default", .action = "send", .run = run_id, .deliveries = r.deliveries, .token = r.token, .expiry_ns = r.expiry_ns, .outcome = "", .result = "done" };
}

fn releaseOf(t: *TestActions, run_id: []const u8, abandoned: bool) ae.Release.Value {
    const r = t.run("default", run_id).?;
    return .{ .ns = "default", .action = "send", .run = run_id, .deliveries = r.deliveries, .token = r.token, .expiry_ns = r.expiry_ns, .abandoned = abandoned, .retry = true, .delay_ms = 5, .@"error" = "boom" };
}

fn extendOf(t: *TestActions, run_id: []const u8) ae.Extend.Value {
    const r = t.run("default", run_id).?;
    return .{ .ns = "default", .action = "send", .run = run_id, .token = r.token, .expiry_ns = r.expiry_ns, .held_since_ns = r.delivered_ns, .lease_ms = r.lease_ms };
}

/// An expire of `item` alone.
fn expiring(buf: []u8, item: ae.ExpireItem.Value) !ae.Expire.Value {
    return .{ .items = .{ .bytes = try ae.ExpireItem.encodeInto(buf, item), .count = 1 } };
}

fn keyOf(run_id: []const u8, expiry_ns: u64) ae.ExpireItem.Value {
    return .{ .ns = "default", .action = "send", .run = run_id, .expiry_ns = expiry_ns };
}

test "actions machine: a run is delivered once, settled by its holder, and a resent settle answers as it did" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expectEqual(@as(usize, 1), t.count(Ready));

    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const leased = t.run("default", "r1").?;
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
    const done = t.run("default", "r1").?;
    try testing.expectEqual(Status.completed, done.status);
    try testing.expectEqualStrings("failure", done.outcome);
    // The settle took its deadline key with it.
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
    const before = t.sums();
    try testing.expect((try t.apply(.action_settle, settle, 31 * ms)).refused() == null);
    try testing.expectEqual(before, t.sums());
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
    const expiry = t.run("default", "r1").?.expiry_ns;
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
        const settle: ae.Settle.Value = .{ .ns = "default", .action = "send", .run = "r1", .deliveries = rel.deliveries, .token = rel.token, .expiry_ns = rel.expiry_ns, .outcome = "", .result = "" };
        try testing.expectEqualStrings("stale report: the run was released", (try t.apply(.action_settle, settle, stamp + 2 * ms)).refused().?.refusal.message);
        var other = rel;
        other.abandoned = !abandon;
        try testing.expect((try t.apply(.action_release, other, stamp + 2 * ms)).refused() != null);
        try testing.expectEqual(before, t.sums());
        due = stamp + ms + 5 * ms;
        stamp = due + ms;
    }
    const done = t.run("default", "r1").?;
    try testing.expectEqual(Status.failed, done.status);
    try testing.expectEqual(@as(u32, 3), done.deliveries);
    try testing.expectEqual(@as(u32, 2), done.failures);
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
    try testing.expectEqual(@as(u32, 0), t.run("default", "r1").?.failures);
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
    try testing.expectEqual(Status.leased, t.run("default", "r1").?.status);

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
    const ceiling = t.run("default", "r1").?.delivered_ns + ae.MAX_HOLD_MS * ms;
    var stamp: u64 = 21 * ms;
    while (stamp < ceiling + 10 * LEASE) : (stamp += LEASE / 4) {
        const r = t.run("default", "r1").?;
        if (r.status != .leased) break;
        _ = try t.apply(.action_extend, extendOf(&t, "r1"), stamp);
        try t.duty(stamp);
        try testing.expect(t.count(Deadlines) <= 1);
        try testing.expect(t.run("default", "r1").?.expiry_ns <= ceiling);
    }
    // Its last extend reached the ceiling, and the lease ran out there.
    const r = t.run("default", "r1").?;
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

    const expiry = t.run("default", "r1").?.expiry_ns;
    try testing.expectEqual(at + LEASE, expiry);
    _ = try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", expiry)), expiry);
    try testing.expectEqual(Status.timed_out, t.run("default", "r1").?.status);
}

test "actions machine: an expire drops a stale key, and ends a live lease with attempts left by readying it at the expire's stamp" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 3);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const old = t.run("default", "r1").?.expiry_ns;
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
    try testing.expectEqual(Status.leased, t.run("default", "r1").?.status);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    const live = t.run("default", "r1").?.expiry_ns;
    const stamp = live + 7 * ms;
    try testing.expect((try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", live)), stamp)).refused() == null);
    const run = t.run("default", "r1").?;
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
    const expiry = t.run("default", "r1").?.expiry_ns;
    try testing.expect((try t.apply(.action_settle, settleOf(&t, "r1"), expiry + ms)).refused() == null);
    const r = try t.apply(.action_expire, try expiring(&ibuf, keyOf("r1", expiry)), expiry + 2 * ms);
    var answer = r.reader();
    try testing.expectEqual(@as(u32, 0), answer.int(u32).?);
    try testing.expectEqual(Status.completed, t.run("default", "r1").?.status);
}

test "actions machine: one run id in two namespaces has two deadline keys" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    for ([_][]const u8{ "default", "other" }) |ns| {
        try registerIn(&t, ns, 1);
        try invokeIn(&t, ns, "r1", 10 * ms);
        try testing.expect((try deliverIn(&t, ns, "r1", 10 * ms, 20 * ms)).refused() == null);
    }
    try testing.expectEqual(@as(usize, 2), t.count(Deadlines));
}

test "actions machine: a lease below the floor, or past the ceiling, doesn't encode" {
    var buf: [ae.Deliver.max_size]u8 = undefined;
    const body: ae.Deliver.Value = .{ .ns = "default", .action = "send", .run = "r1", .due_ns = 0, .holder = "w1", .request_key = null, .lease_ms = 0 };
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
    const e2 = a.run("default", "r2").?.expiry_ns;
    _ = try a.apply(.action_expire, try expiring(&ibuf, keyOf("r2", e2)), e2 + ms);
    _ = try deliver(&a, "r2", e2 + ms, e2 + 2 * ms);
    _ = try a.apply(.action_release, releaseOf(&a, "r2", false), e2 + 3 * ms);
    try testing.expectEqual(Status.failed, a.run("default", "r2").?.status);

    var b = TestActions.init(testing.allocator);
    defer b.deinit();
    try a.replayInto(&b);
    try testing.expectEqual(a.sums(), b.sums());
}
