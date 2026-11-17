//! Children: a child workflow run's start and end, on the child's shard.
//!
//! A parent's step calls a child (`step_call`, kind `workflow`), and the
//! call sends `child_start` to the child workflow's slot. Here it becomes
//! the child's parent record (`workflow.parents`), and this shard's leader
//! starts the child from its own thread (`workflow_start`, proposed by the
//! workflow handler's duty), or, with no such workflow or version, refuses
//! it (`child_refuse`). The parent's call hears from this machine:
//!
//! - `call_reply` `started`, on observing the child's `workflow_start`;
//! - `call_reply` `no_action`, from a `child_refuse`;
//! - `call_done`, on observing the child's `workflow_complete`, with how it
//!   ended and its output. The parent record goes.
//!
//! An observer here sends only on observing an edge entry, never because of
//! a message it applied: a start and a completion are the child's leader's
//! own proposals. So no chain of applies sends, and none needs fuel or
//! `hops`. `child_start` itself sends nothing.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
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
    /// Not started yet: this shard's leader starts it, or refuses it.
    pending = 1,
    /// Its `workflow_start` applied.
    started = 2,
};

/// A child run's parent: where its end goes, and what to start it with.
/// Per-run state keyed like the run, which a workflow on the contract folds
/// into the run's own record as its parent link.
pub const Parent = Record(&.{
    .{ .name = "parent_slot", .T = Int(u16) },
    .{ .name = "parent_run", .T = Bytes(limits.MAX_QUALIFIED_KEY) },
    .{ .name = "parent_workflow", .T = ae.CallerWorkflow },
    .{ .name = "workflow", .T = ae.TargetName },
    .{ .name = "version", .T = ae.Version },
    .{ .name = "input", .T = Bytes(ae.MAX_INPUT) },
    .{ .name = "state", .T = schema.Enum(State) },
});

pub const Parents = view_mod.Family(26, "workflow.parents", .id, delivery.Stored(Parent));

const KeyBuf = [4 + ae.MAX_RUN_ID]u8;

fn parentKey(buf: *KeyBuf, ns: NamespaceId, run: []const u8) ?[]const u8 {
    if (run.len > ae.MAX_RUN_ID) return null;
    return view_mod.idKey(buf, ns, run) catch null;
}

/// Child `run`'s parent record in `ns`, as applied.
pub fn parentOf(read: ReadView, ns: NamespaceId, run: []const u8) ?Parent.Value {
    var kb: KeyBuf = undefined;
    const k = parentKey(&kb, ns, run) orelse return null;
    const v = (read.get(Parents, k) catch null) orelse return null;
    return v.v;
}

fn typeOf(e: *const Entry) ?EntryType {
    return std.enums.fromInt(EntryType, e.header.entry_type);
}

const Named = struct { ns: NamespaceId, run: []const u8 };

/// The child run an entry names: a start or refusal's own fields, or a
/// workflow entry's key (the namespace id, then the run id).
fn named(e: *const Entry) ?Named {
    var diag: schema.Diagnostic = .{};
    return switch (typeOf(e) orelse return null) {
        .child_start => blk: {
            const p = ae.ChildStart.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .run = p.run };
        },
        .child_refuse => blk: {
            const p = ae.ChildRefuse.decodeAll(e.payload, &diag) catch return null;
            break :blk .{ .ns = .of(p.ns), .run = p.run };
        },
        .workflow_start, .workflow_complete => blk: {
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
            if (cmd.key.len < 4) return null;
            break :blk .{ .ns = cmd.namespace_id, .run = cmd.key[4..] };
        },
        else => null,
    };
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const n = named(e) orelse return;
    if (n.ns == .invalid or n.run.len > ae.MAX_RUN_ID) return;
    try out.addIn(Parents.ID, n.ns, n.run);
}

/// The namespace a child's start names, held to its origin's.
fn namespaceOf(e: *const Entry) ?u32 {
    if (typeOf(e) != .child_start) return null;
    const n = named(e) orelse return null;
    return n.ns.int();
}

/// A reply to the parent's call: the child started, or can't be.
fn reply(view: *View, ns: NamespaceId, run: []const u8, p: Parent.Value, answer: ae.Answer) ApplyError!void {
    var buf: [ae.Reply.max_size]u8 = undefined;
    const bytes = ae.Reply.encodeInto(&buf, .{ .ns = ns.int(), .slot = p.parent_slot, .run = run, .answer = answer }) catch return error.Halt;
    try view.send(.user(ns), p.parent_slot, @backingInt(EntryType.call_reply), bytes);
}

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    _ = out;
    var diag: schema.Diagnostic = .{};
    // Only a parent's call makes a start, and only this shard's leader a
    // refusal: one that doesn't decode is a Flo bug.
    const n = named(e) orelse return error.Halt;
    if (n.ns == .invalid) return error.Halt;
    var kb: KeyBuf = undefined;
    const k = parentKey(&kb, n.ns, n.run) orelse return error.Halt;
    const found = try view.get(Parents, k);
    switch (typeOf(e).?) {
        .child_start => {
            const p = ae.ChildStart.decodeAll(e.payload, &diag) catch return error.Halt;
            // Sent once, so a second is another call's: the first stands.
            if (found != null) return;
            try view.put(Parents, k, .{ .v = .{
                .parent_slot = p.parent_slot,
                .parent_run = p.parent_run,
                .parent_workflow = p.parent_workflow,
                .workflow = p.workflow,
                .version = p.version,
                .input = p.input,
                .state = .pending,
            } });
        },
        .child_refuse => {
            // Refused by an earlier leader already, or started meanwhile.
            const f = found orelse return;
            if (f.v.state != .pending) return;
            try reply(view, n.ns, n.run, f.v, .no_action);
            try view.delete(Parents, k);
        },
        else => return error.Halt,
    }
}

/// A child's start or end, applied by the workflow handler: its parent
/// hears. Both are this shard's leader's own proposals, never a message's.
fn observe(view: *View, e: *const Entry) ApplyError!void {
    const n = named(e) orelse return;
    if (n.ns == .invalid) return;
    var kb: KeyBuf = undefined;
    const k = parentKey(&kb, n.ns, n.run) orelse return;
    // A run with no parent here: not a child, or one already ended.
    var p = (try view.get(Parents, k)) orelse return;
    switch (typeOf(e).?) {
        .workflow_start => {
            if (p.v.state != .pending) return;
            p.v.state = .started;
            try view.put(Parents, k, p);
            try reply(view, n.ns, n.run, p.v, .started);
        },
        .workflow_complete => {
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
            const ended = endOf(cmd.value) orelse return;
            const buf = view.arena.alloc(u8, ae.Done.max_size) catch return error.OutOfMemory;
            const bytes = ae.Done.encodeInto(buf, .{
                .ns = n.ns.int(),
                .slot = p.v.parent_slot,
                .run = n.run,
                .status = ended.status,
                .outcome = "",
                .result = ended.output,
            }) catch return error.Halt;
            try view.send(.user(n.ns), p.v.parent_slot, @backingInt(EntryType.call_done), bytes);
            try view.delete(Parents, k);
        },
        else => {},
    }
}

/// Whether a workflow entry is a child's: most runs aren't, and need no
/// view.
fn isChild(read: ReadView, e: *const Entry) bool {
    const n = named(e) orelse return false;
    if (n.ns == .invalid) return false;
    return parentOf(read, n.ns, n.run) != null;
}

/// How a run ended, from its `workflow_complete` value:
/// `[status:u8][has_output:u8]([output_len:u32][output])?...`. An output
/// past what a call's result holds goes as none.
pub const Ended = struct { status: ae.DoneStatus, output: []const u8 };

pub fn endOf(value: []const u8) ?Ended {
    if (value.len < 2) return null;
    const status: ae.DoneStatus = switch (value[0]) {
        @backingInt(RunStatus.completed) => .completed,
        @backingInt(RunStatus.failed) => .failed,
        @backingInt(RunStatus.cancelled) => .cancelled,
        @backingInt(RunStatus.timed_out) => .timed_out,
        else => return null,
    };
    var output: []const u8 = "";
    if (value[1] != 0) {
        if (value.len < 6) return null;
        const len = std.mem.readInt(u32, value[2..6], .little);
        if (value.len < 6 + len) return null;
        if (len <= @import("../protocol/action_ops.zig").MAX_OUTPUT) output = value[6..][0..len];
    }
    return .{ .status = status, .output = output };
}

const RunStatus = @import("handler.zig").WorkflowHandler.RunStatus;

pub const machine: machine_mod.Machine = .{
    .name = "children",
    .entry_types = &.{ .child_start, .child_refuse },
    .families = &.{machine_mod.info(Parents)},
    .keys = keys,
    .apply = apply,
    .namespace = namespaceOf,
    .observes = &.{ .workflow_start, .workflow_complete },
    .observe_needed = isChild,
    .observe_keys = keys,
    .observe = observe,
};
