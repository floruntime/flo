//! Calls: a workflow step's run of an action or a child workflow, asked for
//! over a channel and recorded on the workflow's shard.
//!
//! - A step's call is recorded, and the run's start (`action_invoke`)
//!   sent to the action's slot, by the step that makes it (`record`).
//! - The action's shard answers in the entry that applied the start
//!   (`call_reply`): the run exists, or no such action is registered.
//! - When the run ends, its shard sends `call_done` with what it
//!   returned.
//! - A call whose action wasn't registered is asked for again
//!   (`step_call_retry`), and one its step is done with goes
//!   (`step_call_clear`).
//!
//! A call is keyed (ns, caller slot, run): the slot is the calling
//! workflow's, so a call is the slot's state and moves with it. An answer
//! for a call that's gone (its workflow cancelled, its step resolved)
//! applies nothing: its apply answers 0, which the shard counts.
//!
//! The workflow runs machine (`workflow/runs_machine.zig`) owns these
//! entries and `step.calls`, and applies them here: a step's call is
//! recorded in the same apply as the step that makes it (`record`).

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
const router = @import("../node/router.zig");
const NamespaceId = @import("../namespace/table.zig").Id;
const plant = @import("../plant.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

pub const State = enum(u8) {
    /// Its start is sent, and not answered yet.
    sent = 1,
    /// Its run exists.
    started = 2,
    /// No such action was registered when the start arrived.
    no_action = 3,
    /// Its run ended.
    done = 4,
    /// Its start named another caller's run: it has no run, and won't.
    refused = 5,
    /// Its action's shard was at its memory share when the start arrived:
    /// no run yet, and its start is sent again once memory may have freed.
    over_budget = 6,
};

/// A step's call of an action's run or a child workflow's, kept under the
/// calling workflow's slot until the step is done with it. One record for
/// both kinds.
pub const Call = Record(&.{
    .{ .name = "kind", .T = schema.Enum(ae.CallKind) },
    .{ .name = "caller_run", .T = Bytes(limits.MAX_QUALIFIED_KEY) },
    .{ .name = "caller_workflow", .T = ae.CallerWorkflow },
    .{ .name = "step", .T = ae.StepName },
    .{ .name = "target", .T = ae.TargetName },
    .{ .name = "version", .T = ae.Version },
    .{ .name = "input", .T = Bytes(ae.MAX_INPUT) },
    .{ .name = "can_wait", .T = schema.Bool },
    .{ .name = "state", .T = schema.Enum(State) },
    // Starts sent, the first one's time, and the latest one's.
    .{ .name = "attempts", .T = Int(u32) },
    .{ .name = "first_ns", .T = Int(u64) },
    .{ .name = "last_ns", .T = Int(u64) },
    // Once done: how the run ended, and what it returned.
    .{ .name = "status", .T = Int(u8) },
    .{ .name = "outcome", .T = ae.Outcome },
    .{ .name = "result", .T = Bytes(action_ops.MAX_OUTPUT) },
});

pub const Calls = view_mod.Family(25, "step.calls", .id, delivery.Stored(Call));

/// The slot a workflow's runs are placed on, which its calls' answers go
/// back to: by its name, since a client may choose a run's id.
pub fn callerSlot(ns: NamespaceId, workflow: []const u8) u16 {
    return @intCast(router.nsKeySlot(ns, workflow));
}

const RAW_LEN = 2 + ae.MAX_RUN_ID;
const KeyBuf = [4 + RAW_LEN]u8;

/// A call's key after its namespace's id: the caller's slot, then the run.
fn rawKey(buf: *[RAW_LEN]u8, slot: u16, run: []const u8) ?[]const u8 {
    if (run.len > ae.MAX_RUN_ID) return null;
    std.mem.writeInt(u16, buf[0..2], slot, .big);
    @memcpy(buf[2..][0..run.len], run);
    return buf[0 .. 2 + run.len];
}

fn callKey(buf: *KeyBuf, ns: NamespaceId, slot: u16, run: []const u8) ?[]const u8 {
    var raw: [RAW_LEN]u8 = undefined;
    return view_mod.idKey(buf, ns, rawKey(&raw, slot, run) orelse return null) catch null;
}

/// A call's key in `step.calls`, allocated; null for a run id no call can
/// have.
pub fn callKeyAlloc(a: std.mem.Allocator, ns: NamespaceId, slot: u16, run: []const u8) error{OutOfMemory}!?[]u8 {
    var kb: KeyBuf = undefined;
    const k = callKey(&kb, ns, slot, run) orelse return null;
    return try a.dupe(u8, k);
}

/// The call `run` of the workflow on `slot` in `ns`, as applied.
pub fn callOf(read: ReadView, ns: NamespaceId, slot: u16, run: []const u8) ?Call.Value {
    var kb: KeyBuf = undefined;
    const k = callKey(&kb, ns, slot, run) orelse return null;
    const v = (read.get(Calls, k) catch null) orelse return null;
    return v.v;
}

fn typeOf(e: *const Entry) ?EntryType {
    return std.enums.fromInt(EntryType, e.header.entry_type);
}

/// The key an entry names: (ns, slot, run).
const Named = struct { ns: NamespaceId, slot: u16, run: []const u8 };

fn named(e: *const Entry) ?Named {
    var diag: schema.Diagnostic = .{};
    return switch (typeOf(e) orelse return null) {
        .step_call_retry => blk: {
            const p = ae.CallRetry.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        .step_call_clear => blk: {
            const p = ae.CallClear.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        .call_reply => blk: {
            const p = ae.Reply.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        .call_done => blk: {
            const p = ae.Done.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        else => null,
    };
}

/// Whether `t` is one of the entries applied here.
pub fn owns(t: EntryType) bool {
    return switch (t) {
        .step_call_retry, .step_call_clear, .call_reply, .call_done => true,
        else => false,
    };
}

pub fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const n = named(e) orelse return;
    if (n.ns == .invalid) return;
    var raw: [RAW_LEN]u8 = undefined;
    try out.addIn(Calls.ID, n.ns, rawKey(&raw, n.slot, n.run) orelse return);
}

/// The namespace a message names, held to its origin's.
pub fn namespaceOf(e: *const Entry) ?u32 {
    const n = named(e) orelse return null;
    return n.ns.int();
}

pub const TARGET_TOO_LONG = "the call's target is longer than an action name may be";

/// Why call `c`'s start can't be made, or null: an action's target longer
/// than an action's name has no run to start. Its leader refuses those
/// first; this keeps one that reaches the log from halting it.
fn unstartable(c: Call.Value) ?[]const u8 {
    if (c.kind == .action and c.target.len > ae.MAX_ACTION_NAME) return TARGET_TOO_LONG;
    return null;
}

/// Sends the call's start to its target's slot: an action's run's invoke,
/// or a child workflow's start, which carries where the child's end goes.
fn sendStart(view: *View, ns: NamespaceId, slot: u16, run: []const u8, c: Call.Value) ApplyError!void {
    const to: u16 = @intCast(router.nsKeySlot(ns, c.target));
    switch (c.kind) {
        .action => {
            const buf = view.arena.alloc(u8, ae.Invoke.max_size) catch return error.OutOfMemory;
            const start = ae.Invoke.encodeInto(buf, .{
                .ns = ns.int(),
                .action = c.target,
                .run = run,
                .input = c.input,
                .labels = null,
                .source = .workflow,
                .caller_run = c.caller_run,
                .caller_workflow = c.caller_workflow,
            }) catch return error.Halt;
            try view.send(.user(ns), to, @backingInt(EntryType.action_invoke), start);
        },
        .workflow => {
            const buf = view.arena.alloc(u8, ae.ChildStart.max_size) catch return error.OutOfMemory;
            const start = ae.ChildStart.encodeInto(buf, .{
                .ns = ns.int(),
                .parent_slot = slot,
                .parent_run = c.caller_run,
                .parent_workflow = c.caller_workflow,
                .workflow = c.target,
                .version = c.version,
                .run = run,
                .input = c.input,
            }) catch return error.Halt;
            try view.send(.user(ns), to, @backingInt(EntryType.child_start), start);
        },
    }
}

/// The key a step's call `p` is recorded under, for the step's keys.
pub fn recordKey(a: std.mem.Allocator, p: ae.Call.Value) error{OutOfMemory}!?[]u8 {
    return callKeyAlloc(a, .of(p.ns), callerSlot(.of(p.ns), p.caller_workflow), p.run);
}

/// A step's call `p`, recorded and its start sent, stamped `stamp`: in the
/// apply of the step that makes it. One already there (a step proposed
/// twice) stands.
pub fn record(view: *View, p: ae.Call.Value, stamp: u64) ApplyError!void {
    const ns: NamespaceId = .of(p.ns);
    const slot = callerSlot(ns, p.caller_workflow);
    var kb: KeyBuf = undefined;
    const k = callKey(&kb, ns, slot, p.run) orelse return error.Halt;
    if (try view.get(Calls, k) != null) return;
    const c: Call.Value = .{
        .kind = p.kind,
        .caller_run = p.caller_run,
        .caller_workflow = p.caller_workflow,
        .step = p.step,
        .target = p.target,
        .version = p.version,
        .input = p.input,
        .can_wait = p.can_wait,
        .state = .sent,
        .attempts = 1,
        .first_ns = stamp,
        .last_ns = stamp,
        .status = 0,
        .outcome = "",
        .result = "",
    };
    // One that can't start is recorded refused, saying why, and its step
    // fails as for any refused call.
    if (unstartable(c)) |why| {
        var refused = c;
        refused.state = .refused;
        refused.outcome = why;
        return view.put(Calls, k, .{ .v = refused });
    }
    try view.put(Calls, k, .{ .v = c });
    try sendStart(view, ns, slot, p.run, c);
}

/// Answers: a reply or a done, 1 when it changed a call and 0 when it
/// found none, which the shard counts. The rest answer nothing.
pub fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    // Only a leader makes these entries, and only an actions machine these
    // messages: one that doesn't decode is a Flo bug.
    const n = named(e) orelse return error.Halt;
    if (n.ns == .invalid) return error.Halt;
    var kb: KeyBuf = undefined;
    const k = callKey(&kb, n.ns, n.slot, n.run) orelse return error.Halt;
    const found = try view.get(Calls, k);
    switch (typeOf(e).?) {
        .step_call_retry => {
            var c = (found orelse return).v;
            // Answered since the retry was proposed, or proposed twice.
            if (c.state != .no_action and c.state != .over_budget) return;
            c.attempts +|= 1;
            c.last_ns = stamp;
            if (unstartable(c)) |why| {
                c.state = .refused;
                c.outcome = why;
                return view.put(Calls, k, .{ .v = c });
            }
            c.state = .sent;
            try view.put(Calls, k, .{ .v = c });
            try sendStart(view, n.ns, n.slot, n.run, c);
        },
        .step_call_clear => if (found != null) try view.delete(Calls, k),
        .call_reply => {
            const p = ae.Reply.decodeAll(e.payload, &diag) catch return error.Halt;
            // Reserved for when slots move: until then, a sender's bug.
            if (p.answer == .moved) return out.refuse(.bad_request, .malformed, MOVED_NOT_YET);
            var c = (found orelse return out.putInt(u8, 0)).v;
            out.putInt(u8, 1);
            // A run that ended before its start's answer applied (both are
            // in one inbound, say) stays done.
            if (c.state != .sent) return;
            c.state = switch (p.answer) {
                .started => .started,
                .no_action => .no_action,
                .over_budget => .over_budget,
                .conflict, .skipped, .refused => .refused,
                .moved => unreachable,
            };
            if (p.why) |w| c.outcome = w;
            c.last_ns = stamp;
            try view.put(Calls, k, .{ .v = c });
        },
        .call_done => {
            const p = ae.Done.decodeAll(e.payload, &diag) catch return error.Halt;
            var c = (found orelse return out.putInt(u8, 0)).v;
            out.putInt(u8, 1);
            // A call settles once: a run's end after it settled refused or
            // no action (a skipped `started`, say) leaves it as it was.
            const settled = if (comptime plant.active == .runs_done_overrides_settled) c.state == .done else c.state != .sent and c.state != .started;
            if (settled) return;
            c.state = .done;
            c.status = @backingInt(p.status);
            c.outcome = p.outcome;
            c.result = p.result;
            c.last_ns = stamp;
            try view.put(Calls, k, .{ .v = c });
        },
        else => return error.Halt,
    }
}

pub const MOVED_NOT_YET = "bad request: a call's start answered moved, which nothing sends until slots move";

/// An answer to one of this shard's calls that a repair skipped: the call
/// settles refused, saying so, so its step fails rather than waits for an
/// answer that will never come. One already settled stays as it is. A
/// skipped `started` reply fails the step while the callee's run, which
/// did start, still executes: its end is answered to a call already
/// settled, and changes nothing.
pub fn skipped(view: *View, e: *const Entry, why: []const u8) ApplyError!void {
    const n = named(e) orelse return;
    var kb: KeyBuf = undefined;
    const k = callKey(&kb, n.ns, n.slot, n.run) orelse return;
    var c = ((try view.get(Calls, k)) orelse return).v;
    if (c.state == .done or c.state == .refused or c.state == .no_action or c.state == .over_budget) return;
    c.state = .refused;
    c.outcome = why;
    c.last_ns = e.header.timestamp_ns;
    try view.put(Calls, k, .{ .v = c });
}
