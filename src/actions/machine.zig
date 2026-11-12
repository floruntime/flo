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
const ae = @import("../protocol/action_entries.zig");
const action_ops = @import("../protocol/action_ops.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");

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
    .{ .name = "caller_shard", .T = Optional(Int(u16)) },
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
    .{ .name = "result", .T = Bytes(action_ops.MAX_OUTPUT) },
    .{ .name = "error", .T = Bytes(action_ops.MAX_ERROR) },
});

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

pub const Defs = view_mod.Family(20, "action.def", Stored(Def));
pub const Runs = view_mod.Family(21, "action.run", Stored(Run));
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
        },
        .action_deliver => {
            const p = ae.Deliver.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Defs.ID, ns, p.action);
            try out.addIn(Runs.ID, ns, p.run);
            try out.addIn(Ready.ID, ns, readyKey(&b, p.action, {}, p.due_ns, p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, after(stamp, p.lease_ms), ns, p.run));
            if (p.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, p.holder, k));
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Runs.ID, ns, p.run);
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, ns, p.run));
            if (p.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, p.holder, k));
        },
        .action_release => {
            const p = ae.Release.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try out.addIn(Runs.ID, ns, p.run);
            try out.addIn(Ready.ID, ns, readyKey(&b, p.action, {}, after(stamp, p.delay_ms), p.run));
            try out.add(Deadlines.ID, "", deadlineKey(&b, p.expiry_ns, ns, p.run));
            if (p.request_key) |k| try out.addIn(Requests.ID, ns, requestKey(&rb, p.holder, k));
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
            if (std.mem.indexOfScalar(u8, p.name, 0) != null) return out.refuse(.bad_request, .malformed, NAME_HAS_NUL);
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
            const def = (try view.get(Defs, try idKey(view, .of(p.ns), p.action))) orelse return out.refuse(.not_found, .not_found, NO_ACTION);
            const rk = try idKey(view, .of(p.ns), p.run);
            if (try view.get(Runs, rk) != null) return out.refuse(.conflict, .unclassified, RUN_EXISTS);
            try view.put(Runs, rk, .{ .v = .{
                .action = p.action,
                .status = .ready,
                .input = p.input,
                .labels = p.labels,
                .source = p.source,
                .caller_run = p.caller_run,
                .caller_workflow = p.caller_workflow,
                .caller_shard = p.caller_shard,
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
                .@"error" = "",
            } });
            var b: KeyBuf = undefined;
            try view.put(Ready, try idKey(view, .of(p.ns), readyKey(&b, p.action, {}, stamp, p.run)), .{});
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
            try M.deliver(view, .of(p.ns), p.run, &run.v, p.holder, p.request_key, .{ .term = e.header.term, .index = e.header.index }, p.lease_ms, stamp);
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
                if (sameDelivery(run.v, p.holder, p.deliveries, p.token) and run.v.ended == .settled) {
                    if (std.mem.eql(u8, run.v.outcome, p.outcome) and std.mem.eql(u8, run.v.result, p.result)) return;
                    return out.refuse(.conflict, .stale_report, OTHER_RESULT);
                }
                return stale(out, run.v, p.holder, p.deliveries, p.token);
            }
            try M.endLease(view, .of(p.ns), p.run, run.v, p.holder, p.request_key, p.expiry_ns);
            run.v.status = .completed;
            run.v.ended = .settled;
            run.v.outcome = p.outcome;
            run.v.result = p.result;
            run.v.@"error" = "";
            run.v.completed_ns = stamp;
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
            try M.endLease(view, .of(p.ns), p.run, run.v, p.holder, p.request_key, p.expiry_ns);
            run.v.@"error" = p.@"error";
            if (p.abandoned) {
                run.v.ended = .abandoned;
                try M.readyAgain(view, .of(p.ns), p.run, &run.v, after(stamp, p.delay_ms));
            } else {
                run.v.ended = .released;
                run.v.failures +|= 1;
                if (!p.retry or run.v.failures >= run.v.max_attempts) {
                    run.v.status = .failed;
                    run.v.completed_ns = stamp;
                } else try M.readyAgain(view, .of(p.ns), p.run, &run.v, after(stamp, p.delay_ms));
            }
            try view.put(Runs, rk, run);
        },
        .action_extend => {
            const p = ae.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try idKey(view, .of(p.ns), p.run);
            var run = (try inScope(view, rk, p.action)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.holder, run.v.deliveries, p.token)) return stale(out, run.v, p.holder, run.v.deliveries, p.token);
            if (p.lease_ms != run.v.lease_ms or p.held_since_ns != run.v.delivered_ns) return out.refuse(.bad_request, .unclassified, NOT_ITS_LEASE);
            // More than half the lease left, or held to the ceiling: the
            // lease stands, and nothing is written.
            const to = (try M.extend(view, .of(p.ns), p.run, &run.v, p.expiry_ns, stamp)) orelse return out.putInt(u64, run.v.expiry_ns);
            try view.put(Runs, rk, run);
            out.putInt(u64, to);
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
    try M.dropRequest(view, .of(item.ns), item.holder, item.request_key, item.run);
    run.v.ended = .expired;
    run.v.failures +|= 1;
    run.v.@"error" = "lease expired";
    if (run.v.failures >= run.v.max_attempts) {
        run.v.status = .timed_out;
        run.v.completed_ns = stamp;
    } else try M.readyAgain(view, .of(item.ns), item.run, &run.v, stamp);
    try view.put(Runs, rk, run);
    return true;
}

/// The run at `rk`, if it is `action`'s.
fn inScope(view: *View, rk: []const u8, action: []const u8) ApplyError!?Stored(Run) {
    const run = (try view.get(Runs, rk)) orelse return null;
    return if (std.mem.eql(u8, run.v.action, action)) run else null;
}

pub const OTHER_RESULT = "stale report: the run was completed with another result";

const holds = delivery.holds;
const sameDelivery = delivery.sameDelivery;
const stale = M.stale;
pub const staleWhy = M.staleWhy;
/// `k` in `ns`; an entry naming no namespace (`.invalid`) halts.
const idKey = M.inNs;

pub const machine: machine_mod.Machine = .{
    .name = "actions",
    .entry_types = &.{ .action_register, .action_delete, .action_invoke, .action_deliver, .action_settle, .action_release, .action_extend, .action_expire },
    .families = &.{ machine_mod.info(Defs), machine_mod.info(Runs), machine_mod.info(Ready), machine_mod.info(Deadlines), machine_mod.info(Requests) },
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
    const r = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = ns.int(), .action = "send", .run = run_id, .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null, .caller_shard = null }, stamp);
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
    return .{ .ns = DEFAULT, .action = "send", .run = run_id, .holder = "w1", .token = r.token, .expiry_ns = r.expiry_ns, .held_since_ns = r.delivered_ns, .lease_ms = r.lease_ms };
}

/// An expire of `item` alone.
fn expiring(buf: []u8, item: ae.ExpireItem.Value) !ae.Expire.Value {
    return .{ .items = .{ .bytes = try ae.ExpireItem.encodeInto(buf, item), .count = 1 } };
}

fn keyOf(run_id: []const u8, expiry_ns: u64) ae.ExpireItem.Value {
    return .{ .ns = DEFAULT, .action = "send", .run = run_id, .expiry_ns = expiry_ns, .holder = "w1", .request_key = null };
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
    try testing.expectEqualStrings(NO_ACTION, (try t.apply(.action_invoke, ae.Invoke.Value{ .ns = DEFAULT, .action = "send", .run = "r2", .input = "", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null, .caller_shard = null }, 40 * ms)).refused().?.refusal.message);
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
