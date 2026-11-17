//! Calls: a workflow step's action run, asked for over a channel and
//! recorded on the workflow's shard.
//!
//! - `action_call` records the call and sends the run's start
//!   (`action_invoke`) to the action's slot.
//! - The action's shard answers in the entry that applied the start
//!   (`action_reply`): the run exists, or no such action is registered.
//! - When the run ends, its shard sends `action_done` with what it
//!   returned.
//! - A call whose action wasn't registered is asked for again
//!   (`action_call_retry`), and one its step is done with goes
//!   (`action_call_clear`).
//!
//! A call is keyed (ns, caller slot, run): the slot is the calling
//! workflow's, so a call is the slot's state and moves with it. An answer
//! for a call that's gone (its workflow cancelled, its step resolved)
//! applies nothing: its apply answers 0, which the shard counts.

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
};

pub const Call = Record(&.{
    .{ .name = "caller_run", .T = Bytes(limits.MAX_QUALIFIED_KEY) },
    .{ .name = "caller_workflow", .T = ae.CallerWorkflow },
    .{ .name = "step", .T = ae.StepName },
    .{ .name = "action", .T = ae.ActionName },
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

pub const Calls = view_mod.Family(25, "action.calls", .id, delivery.Stored(Call));

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
        .action_call => blk: {
            const p = ae.Call.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = callerSlot(.of(p.ns), p.caller_workflow), .run = p.run };
        },
        .action_call_retry => blk: {
            const p = ae.CallRetry.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        .action_call_clear => blk: {
            const p = ae.CallClear.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        .action_reply => blk: {
            const p = ae.Reply.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        .action_done => blk: {
            const p = ae.Done.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .slot = p.slot, .run = p.run };
        },
        else => null,
    };
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const n = named(e) orelse return;
    if (n.ns == .invalid) return;
    var raw: [RAW_LEN]u8 = undefined;
    try out.addIn(Calls.ID, n.ns, rawKey(&raw, n.slot, n.run) orelse return);
}

/// The namespace a message names, held to its origin's.
fn namespaceOf(e: *const Entry) ?u32 {
    const n = named(e) orelse return null;
    return n.ns.int();
}

/// Sends the run's start for `call` to the action's slot.
fn sendStart(view: *View, ns: NamespaceId, run: []const u8, c: Call.Value) ApplyError!void {
    const buf = view.arena.alloc(u8, ae.Invoke.max_size) catch return error.OutOfMemory;
    const start = ae.Invoke.encodeInto(buf, .{
        .ns = ns.int(),
        .action = c.action,
        .run = run,
        .input = c.input,
        .labels = null,
        .source = .workflow,
        .caller_run = c.caller_run,
        .caller_workflow = c.caller_workflow,
    }) catch return error.Halt;
    try view.send(.user(ns), @intCast(router.nsKeySlot(ns, c.action)), @backingInt(EntryType.action_invoke), start);
}

/// Answers: a reply or a done, 1 when it changed a call and 0 when it
/// found none, which the shard counts. The rest answer nothing.
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
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
        .action_call => {
            const p = ae.Call.decodeAll(e.payload, &diag) catch return error.Halt;
            // Proposed twice before it applied: the first stands.
            if (found != null) return;
            const c: Call.Value = .{
                .caller_run = p.caller_run,
                .caller_workflow = p.caller_workflow,
                .step = p.step,
                .action = p.action,
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
            try view.put(Calls, k, .{ .v = c });
            try sendStart(view, n.ns, n.run, c);
        },
        .action_call_retry => {
            var c = (found orelse return).v;
            // Answered since the retry was proposed, or proposed twice.
            if (c.state != .no_action) return;
            c.state = .sent;
            c.attempts +|= 1;
            c.last_ns = stamp;
            try view.put(Calls, k, .{ .v = c });
            try sendStart(view, n.ns, n.run, c);
        },
        .action_call_clear => if (found != null) try view.delete(Calls, k),
        .action_reply => {
            const p = ae.Reply.decodeAll(e.payload, &diag) catch return error.Halt;
            var c = (found orelse return out.putInt(u8, 0)).v;
            out.putInt(u8, 1);
            // A run that ended before its start's answer applied (both are
            // in one inbound, say) stays done.
            if (c.state != .sent) return;
            c.state = switch (p.answer) {
                .started => .started,
                .no_action => .no_action,
            };
            c.last_ns = stamp;
            try view.put(Calls, k, .{ .v = c });
        },
        .action_done => {
            const p = ae.Done.decodeAll(e.payload, &diag) catch return error.Halt;
            var c = (found orelse return out.putInt(u8, 0)).v;
            out.putInt(u8, 1);
            if (c.state == .done) return;
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

pub const machine: machine_mod.Machine = .{
    .name = "calls",
    .entry_types = &.{ .action_call, .action_call_retry, .action_call_clear, .action_reply, .action_done },
    .families = &.{machine_mod.info(Calls)},
    .keys = keys,
    .apply = apply,
    .namespace = namespaceOf,
};
