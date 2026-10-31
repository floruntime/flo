//! Actions on the contract: definitions and runs are four families,
//! changed only through a `View` in `apply`, so a refused entry changes
//! nothing and every replica holds the same runs.
//!
//! - `action.def`: each action's definition, keyed (ns, name).
//! - `action.run`: each run, keyed (ns, run id).
//! - `action.ready`: each ready run, keyed (ns, action, due, run), so a
//!   leader picks the next run of an action without a scan.
//! - `action.deadline`: each lease's deadline, keyed (expiry, run) in no
//!   namespace, so the expire duty reads the earliest without a scan.
//!
//! An entry's keys must follow from the entry alone, and a settle can't
//! know its run's expiry while an extend may be in flight. So deliver and
//! extend each write a deadline key, settles and releases leave theirs,
//! and the expire duty cleans up: it reads keys past due by the log's
//! clock and names each in an expire entry, which deletes every one it
//! names. A key is its lease's identity: a run leased until exactly that
//! instant is the lease it names, and ends; any other key just goes.
//!
//! The bound: a key lives until its own expiry passes and the duty's next
//! tick, and a lease writes one key per deliver or extend. Touches extend
//! at most once per half lease, so each lease holds at most three keys,
//! and keys outlive their lease by at most one lease length.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const action_ops = @import("../protocol/action_ops.zig");
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
    .{ .name = "caller_run", .T = Optional(Bytes(@import("../protocol/limits.zig").MAX_QUALIFIED_KEY)) },
    .{ .name = "caller_workflow", .T = Optional(Bytes(@import("../protocol/limits.zig").MAX_QUALIFIED_KEY)) },
    .{ .name = "created_ns", .T = Int(u64) },
    // Its ready key's due time, while ready.
    .{ .name = "due_ns", .T = Int(u64) },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "failures", .T = Int(u32) },
    .{ .name = "max_attempts", .T = Int(u32) },
    // The latest delivery: its holder, token, deadline and extends.
    .{ .name = "holder", .T = ae.Holder },
    .{ .name = "token", .T = ae.Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "ended", .T = schema.Enum(Ended) },
    .{ .name = "started_ns", .T = Int(u64) },
    .{ .name = "completed_ns", .T = Int(u64) },
    .{ .name = "outcome", .T = schema.Enum(ae.Outcome) },
    .{ .name = "result", .T = Bytes(action_ops.MAX_OUTPUT) },
    .{ .name = "error", .T = Bytes(action_ops.MAX_ERROR) },
});

/// Where a deadline's run is, so the duty can name its keys.
pub const Deadline = Record(&.{
    .{ .name = "ns", .T = ae.Ns },
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

/// The longest key a family here holds, namespace prefix included.
const MAX_KEY = 1 + ae.Ns.max_size + ae.MAX_ACTION_NAME + 1 + 8 + ae.MAX_RUN_ID;

const KeyBuf = [MAX_KEY]u8;

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

/// `[expiry:u64 BE][run]`, in no namespace: every lease, earliest first.
pub fn deadlineKey(buf: []u8, expiry_ns: u64, run: []const u8) []const u8 {
    std.mem.writeInt(u64, buf[0..8], expiry_ns, .big);
    @memcpy(buf[8..][0..run.len], run);
    return buf[0 .. 8 + run.len];
}

/// The prefix of an action's ready keys: `[action][0]`.
pub fn readyPrefix(buf: []u8, action: []const u8) []const u8 {
    @memcpy(buf[0..action.len], action);
    buf[action.len] = 0;
    return buf[0 .. action.len + 1];
}

fn after(stamp_ns: u64, millis: u64) u64 {
    return stamp_ns +| (millis *| std.time.ns_per_ms);
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
            try out.add(Deadlines.ID, "", deadlineKey(&b, after(stamp, p.lease_ms), p.run));
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return;
            try out.add(Runs.ID, p.ns, p.run);
        },
        .action_release => {
            const p = ae.Release.decodeAll(e.payload, &diag) catch return;
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Ready.ID, p.ns, readyKey(&b, p.action, after(stamp, p.delay_ms), p.run));
        },
        .action_extend => {
            const p = ae.Extend.decodeAll(e.payload, &diag) catch return;
            try out.add(Runs.ID, p.ns, p.run);
            try out.add(Deadlines.ID, "", deadlineKey(&b, after(stamp, p.lease_ms), p.run));
        },
        .action_expire => {
            const p = ae.Expire.decodeAll(e.payload, &diag) catch return;
            var it = p.items.iterator();
            while (it.next() catch return) |item| {
                try out.add(Runs.ID, item.ns, item.run);
                try out.add(Deadlines.ID, "", deadlineKey(&b, item.expiry_ns, item.run));
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

/// Answers: deliver, the run's delivery count (u32), which with the
/// entry's (term, index) is the delivery id. The rest answer nothing.
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .action_register => {
            const p = ae.Register.decodeAll(e.payload, &diag) catch return error.Halt;
            if (p.ns.len == 0) return error.Halt;
            if (std.mem.indexOfScalar(u8, p.name, 0) != null) return out.refuse(.bad_request, .malformed, NAME_HAS_NUL);
            const created = if (try view.get(Defs, try nsKey(view, p.ns, p.name))) |d| d.v.created_ns else stamp;
            try view.put(Defs, try nsKey(view, p.ns, p.name), .{ .v = .{ .owner = p.owner, .version = p.version, .lease_ms = p.lease_ms, .max_attempts = p.max_attempts, .created_ns = created } });
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
                .expiry_ns = 0,
                .ended = .none,
                .started_ns = 0,
                .completed_ns = 0,
                .outcome = .success,
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
            var run = (try view.get(Runs, rk)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            // The ready key the leader chose by must still be the run's,
            // and due by the log's clock.
            if (run.v.status != .ready or run.v.due_ns != p.due_ns or p.due_ns > stamp or !std.mem.eql(u8, run.v.action, p.action))
                return out.refuse(.conflict, .unclassified, NOT_READY);
            var b: KeyBuf = undefined;
            try view.delete(Ready, try nsKey(view, p.ns, readyKey(&b, p.action, p.due_ns, p.run)));
            run.v.status = .leased;
            run.v.deliveries +|= 1;
            run.v.holder = p.holder;
            run.v.token = .{ .term = e.header.term, .index = e.header.index };
            run.v.expiry_ns = after(stamp, p.lease_ms);
            run.v.ended = .none;
            if (run.v.started_ns == 0) run.v.started_ns = stamp;
            try view.put(Runs, rk, run);
            try putDeadline(view, p.ns, p.action, p.run, run.v);
            out.putInt(u32, run.v.deliveries);
        },
        .action_settle => {
            const p = ae.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
            const rk = try nsKey(view, p.ns, p.run);
            var run = (try view.get(Runs, rk)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.deliveries, p.token)) {
                // A resend of the settle that applied answers as it did.
                if (sameDelivery(run.v, p.deliveries, p.token) and run.v.ended == .settled) return;
                return stale(out, run.v, p.deliveries, p.token);
            }
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
            var run = (try view.get(Runs, rk)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, p.deliveries, p.token)) {
                const ended: Ended = if (p.abandoned) .abandoned else .released;
                if (sameDelivery(run.v, p.deliveries, p.token) and run.v.ended == ended) return;
                return stale(out, run.v, p.deliveries, p.token);
            }
            if (!std.mem.eql(u8, run.v.action, p.action)) return error.Halt;
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
            var run = (try view.get(Runs, rk)) orelse return out.refuse(.not_found, .not_found, NO_RUN);
            if (!holds(run.v, run.v.deliveries, p.token)) return stale(out, run.v, run.v.deliveries, p.token);
            run.v.expiry_ns = after(stamp, p.lease_ms);
            try view.put(Runs, rk, run);
            try putDeadline(view, p.ns, run.v.action, p.run, run.v);
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

/// One expire item: past due by the log's clock, its key goes; if it was
/// the run's live deadline, the lease ends as a failure. A key not yet
/// due is left alone.
fn expireOne(view: *View, item: ae.ExpireItem.Value, stamp: u64) ApplyError!bool {
    if (item.expiry_ns > stamp) return false;
    var b: KeyBuf = undefined;
    try view.delete(Deadlines, try nsKey(view, "", deadlineKey(&b, item.expiry_ns, item.run)));
    const rk = try nsKey(view, item.ns, item.run);
    var run = (try view.get(Runs, rk)) orelse return false;
    if (run.v.status != .leased or run.v.expiry_ns != item.expiry_ns) return false;
    if (!std.mem.eql(u8, run.v.action, item.action)) return error.Halt;
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

fn readyAgain(view: *View, ns: []const u8, run_id: []const u8, run: *Stored(Run), due_ns: u64) ApplyError!void {
    run.v.status = .ready;
    run.v.due_ns = due_ns;
    var b: KeyBuf = undefined;
    try view.put(Ready, try nsKey(view, ns, readyKey(&b, run.v.action, due_ns, run_id)), .{});
}

fn putDeadline(view: *View, ns: []const u8, action: []const u8, run_id: []const u8, run: Run.Value) ApplyError!void {
    // An extend or a redeliver landing on the same instant rewrites the
    // same key: still one deadline for the one live lease.
    var b: KeyBuf = undefined;
    try view.put(Deadlines, try nsKey(view, "", deadlineKey(&b, run.expiry_ns, run_id)), .{ .v = .{ .ns = ns, .action = action } });
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
/// machine as a shard applies them, one index after another.
pub const TestActions = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,
    term: u64 = 1,

    const store_mod = @import("../contract/store.zig");

    pub fn init(allocator: std.mem.Allocator) TestActions {
        return .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *TestActions) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    /// `body` as the next entry of type `etype`, stamped `stamp_ns`.
    pub fn apply(self: *TestActions, comptime etype: EntryType, body: anytype, stamp_ns: u64) !Result {
        const P = PayloadOf(etype);
        const buf = try self.store.allocator.alloc(u8, try P.encodedSize(body));
        defer self.store.allocator.free(buf);
        const payload = try P.encodeInto(buf, body);
        self.index += 1;
        const e = entry_mod.buildEntry(etype, entry_mod.Flags.NONE, self.term, self.index, stamp_ns, payload);
        var out: Result = .{};
        out.reset(&e);
        try machine_mod.applyEntry(&machine, &self.store, &self.scratch, &e, &out);
        return out;
    }

    pub fn run(self: *TestActions, ns: []const u8, id: []const u8) ?Run.Value {
        var b: [MAX_KEY + 1 + ae.Ns.max_size]u8 = undefined;
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

    fn PayloadOf(comptime etype: EntryType) type {
        return ae.Payload(@backingInt(etype));
    }
};

const testing = std.testing;
const ms = std.time.ns_per_ms;

fn register(t: *TestActions, max_attempts: u32) !void {
    const r = try t.apply(.action_register, ae.Register.Value{ .ns = "default", .name = "send", .owner = "", .version = 1, .lease_ms = 1000, .max_attempts = max_attempts }, 1);
    try testing.expect(r.refused() == null);
}

fn invoke(t: *TestActions, run_id: []const u8, stamp: u64) !void {
    const r = try t.apply(.action_invoke, ae.Invoke.Value{ .ns = "default", .action = "send", .run = run_id, .input = "in", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null }, stamp);
    try testing.expect(r.refused() == null);
}

fn deliver(t: *TestActions, run_id: []const u8, due: u64, stamp: u64) !Result {
    return t.apply(.action_deliver, ae.Deliver.Value{ .ns = "default", .action = "send", .run = run_id, .due_ns = due, .holder = "w1", .request_key = null, .lease_ms = 1000 }, stamp);
}

/// An expire of `item` alone.
fn expiring(buf: []u8, item: ae.ExpireItem.Value) !ae.Expire.Value {
    return .{ .items = .{ .bytes = try ae.ExpireItem.encodeInto(buf, item), .count = 1 } };
}

fn tokenOf(t: *TestActions, run_id: []const u8) ae.Token.Value {
    return t.run("default", run_id).?.token;
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
    try testing.expectEqual(@as(u64, 1020 * ms), leased.expiry_ns);
    try testing.expectEqual(@as(usize, 0), t.count(Ready));
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    // The same ready key can't be delivered twice.
    try testing.expect((try deliver(&t, "r1", 10 * ms, 21 * ms)).refused() != null);

    const settle: ae.Settle.Value = .{ .ns = "default", .run = "r1", .deliveries = 1, .token = tokenOf(&t, "r1"), .outcome = .success, .result = "done" };
    try testing.expect((try t.apply(.action_settle, settle, 30 * ms)).refused() == null);
    try testing.expectEqual(Status.completed, t.run("default", "r1").?.status);
    const before = t.sums();
    try testing.expect((try t.apply(.action_settle, settle, 31 * ms)).refused() == null);
    try testing.expectEqual(before, t.sums());
}

test "actions machine: a refused entry changes nothing: a stale token, an early deliver, an expire before its deadline" {
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
    const token = tokenOf(&t, "r1");
    before = t.sums();
    const wrong: ae.Token.Value = .{ .term = token.term, .index = token.index + 1 };
    const settled = (try t.apply(.action_settle, ae.Settle.Value{ .ns = "default", .run = "r1", .deliveries = 1, .token = wrong, .outcome = .success, .result = "" }, 30 * ms)).refused().?;
    try testing.expectEqual(@import("../protocol/proto.zig").Reason.stale_report, settled.refusal.reason);
    try testing.expect((try t.apply(.action_extend, ae.Extend.Value{ .ns = "default", .run = "r1", .token = wrong, .lease_ms = 5000 }, 30 * ms)).refused() != null);
    try testing.expectEqual(before, t.sums());

    // An expire before the deadline leaves the lease and its key.
    const item: ae.ExpireItem.Value = .{ .ns = "default", .action = "send", .run = "r1", .expiry_ns = 1020 * ms };
    _ = try t.apply(.action_expire, try expiring(&ibuf, item), 1019 * ms);
    try testing.expectEqual(before, t.sums());
}

test "actions machine: failures count to max_attempts; an abandoned delivery counts none" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    try register(&t, 2);
    try invoke(&t, "r1", 10 * ms);

    var due: u64 = 10 * ms;
    var stamp: u64 = 20 * ms;
    for ([_]bool{ true, false, false }) |abandon| {
        try testing.expect((try deliver(&t, "r1", due, stamp)).refused() == null);
        const r = t.run("default", "r1").?;
        const rel: ae.Release.Value = .{ .ns = "default", .action = "send", .run = "r1", .deliveries = r.deliveries, .token = r.token, .abandoned = abandon, .retry = true, .delay_ms = 5, .@"error" = "boom" };
        try testing.expect((try t.apply(.action_release, rel, stamp + ms)).refused() == null);
        // A resend answers as the first did, and changes nothing.
        const before = t.sums();
        try testing.expect((try t.apply(.action_release, rel, stamp + 2 * ms)).refused() == null);
        try testing.expectEqual(before, t.sums());
        due = stamp + ms + 5 * ms;
        stamp = due + ms;
    }
    const done = t.run("default", "r1").?;
    try testing.expectEqual(Status.failed, done.status);
    try testing.expectEqual(@as(u32, 3), done.deliveries);
    try testing.expectEqual(@as(u32, 2), done.failures);
}

test "actions machine: an extend moves the deadline; the old key is dropped and the live lease ends at its own" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 1);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    const token = tokenOf(&t, "r1");
    try testing.expect((try t.apply(.action_extend, ae.Extend.Value{ .ns = "default", .run = "r1", .token = token, .lease_ms = 5000 }, 500 * ms)).refused() == null);
    try testing.expectEqual(@as(usize, 2), t.count(Deadlines));

    // The first deadline passes: its key goes, the lease stays.
    const old: ae.ExpireItem.Value = .{ .ns = "default", .action = "send", .run = "r1", .expiry_ns = 1020 * ms };
    _ = try t.apply(.action_expire, try expiring(&ibuf, old), 1100 * ms);
    try testing.expectEqual(Status.leased, t.run("default", "r1").?.status);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    // The extended one ends it, timed out on its only attempt.
    const live: ae.ExpireItem.Value = .{ .ns = "default", .action = "send", .run = "r1", .expiry_ns = 5500 * ms };
    _ = try t.apply(.action_expire, try expiring(&ibuf, live), 5500 * ms);
    const r = t.run("default", "r1").?;
    try testing.expectEqual(Status.timed_out, r.status);
    try testing.expectEqual(Ended.expired, r.ended);
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));

    // The holder's late settle is told what happened.
    const late = (try t.apply(.action_settle, ae.Settle.Value{ .ns = "default", .run = "r1", .deliveries = 1, .token = token, .outcome = .success, .result = "" }, 5600 * ms)).refused().?;
    try testing.expectEqualStrings("stale report: the lease expired and the run moved on", late.refusal.message);
}

test "actions machine: an extend to the same instant keeps one deadline, and the lease still expires at it" {
    var t = TestActions.init(testing.allocator);
    defer t.deinit();
    var ibuf: [ae.ExpireItem.max_size]u8 = undefined;
    try register(&t, 1);
    try invoke(&t, "r1", 10 * ms);
    try testing.expect((try deliver(&t, "r1", 10 * ms, 20 * ms)).refused() == null);
    // Deadline 1020 ms; an extend at 520 ms for 500 ms lands on it again.
    try testing.expect((try t.apply(.action_extend, ae.Extend.Value{ .ns = "default", .run = "r1", .token = tokenOf(&t, "r1"), .lease_ms = 500 }, 520 * ms)).refused() == null);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));

    const item: ae.ExpireItem.Value = .{ .ns = "default", .action = "send", .run = "r1", .expiry_ns = 1020 * ms };
    _ = try t.apply(.action_expire, try expiring(&ibuf, item), 1020 * ms);
    try testing.expectEqual(Status.timed_out, t.run("default", "r1").?.status);
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
}
