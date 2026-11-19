//! The workflow runs machine: each run as its log made it, and a child
//! run's parent.
//!
//! - `workflow.run`, keyed (namespace id, run id): what started it, how it
//!   stands, and, once it ended, how and with what output.
//! - `workflow.hist`, its history: the last `HIST_KEEP` events, by seq.
//! - `workflow.out`, each step's outcome and output, by step name.
//! - `workflow.idem`: the run each (workflow, idempotency key) started, so
//!   a later start with that key is answered with it.
//! - `workflow.per_def`, per workflow: its runs not yet ended (what a
//!   schedule's `max_concurrent` counts) and the last schedule slot a start
//!   fired. A trigger's cursor is its producer's, moved by each start
//!   applied (`WorkflowHandler.followTrigger`): a redefinition onto another
//!   stream reads that stream from the start.
//! - `workflow.parents`: a child run's parent, from the parent's call
//!   (`child_start`) until the child ends.
//!
//! A child's parent hears from here: a `call_reply`, `started` once its
//! start applies or `refused` when its workflow is disabled; `no_action` or `conflict`
//! from a `child_refuse` this shard's leader proposed; `call_done` with how
//! it ended. The start and the completion are this shard's leader's own
//! proposals, never messages.
//!
//! What step a running run is at, and what it waits for, is still its
//! leader's (`WorkflowHandler`); the completion carries the history and
//! step outputs the leader kept.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const defs_machine = @import("defs_machine.zig");
const parser = @import("parser.zig");
const jsonpath = @import("jsonpath.zig");
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
const Stored = delivery.Stored;
const MAX_PERSIST_PAYLOAD = persistence.MAX_PERSIST_PAYLOAD;

const log = @import("stdx").log;
const plant = @import("../plant.zig");

pub const RunStatus = enum(u8) {
    pending = 0,
    running = 1,
    waiting = 2,
    completed = 3,
    failed = 4,
    cancelled = 5,
    timed_out = 6,

    pub fn toString(self: RunStatus) []const u8 {
        return @tagName(self);
    }

    pub fn isTerminal(self: RunStatus) bool {
        return switch (self) {
            .pending, .running, .waiting => false,
            .completed, .failed, .cancelled, .timed_out => true,
        };
    }
};

/// The events a run's history keeps; each one past it drops the oldest.
pub const HIST_KEEP: u64 = 256;

pub const Run = Record(&.{
    .{ .name = "workflow", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "version", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "input", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // Empty for none.
    .{ .name = "idempotency_key", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "status", .T = Int(u8) },
    .{ .name = "created_at_ms", .T = Int(i64) },
    // 0 until it ends.
    .{ .name = "completed_at_ms", .T = Int(i64) },
    // Whether its first step was taken: one `workflow_step_started` takes
    // it, and a second is refused.
    .{ .name = "stepped", .T = Int(u8) },
    .{ .name = "has_output", .T = Int(u8) },
    .{ .name = "output", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "search_tags", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // The seq the next history event takes, and how many were dropped to
    // keep `HIST_KEEP`.
    .{ .name = "hist_next", .T = Int(u64) },
    .{ .name = "hist_dropped", .T = Int(u64) },
});

pub const Hist = Record(&.{
    .{ .name = "event", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "detail", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "at_ms", .T = Int(i64) },
});

pub const Out = Record(&.{
    .{ .name = "outcome", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "output", .T = Bytes(MAX_PERSIST_PAYLOAD) },
});

pub const Idem = Record(&.{
    .{ .name = "run", .T = Bytes(MAX_PERSIST_PAYLOAD) },
});

pub const PerDef = Record(&.{
    .{ .name = "active", .T = Int(u32) },
    // The latest schedule slot a start fired; 0 for none.
    .{ .name = "fired_ms", .T = Int(i64) },
});

pub const ParentState = enum(u8) {
    /// Not started yet: this shard's leader starts it, or refuses it.
    pending = 1,
    /// Its `workflow_start` applied.
    started = 2,
};

/// A child run's parent: where its end goes, and what to start it with.
pub const Parent = Record(&.{
    .{ .name = "parent_slot", .T = Int(u16) },
    .{ .name = "parent_run", .T = Bytes(limits.MAX_QUALIFIED_KEY) },
    .{ .name = "parent_workflow", .T = ae.CallerWorkflow },
    .{ .name = "workflow", .T = ae.TargetName },
    .{ .name = "version", .T = ae.Version },
    .{ .name = "input", .T = Bytes(ae.MAX_INPUT) },
    .{ .name = "state", .T = schema.Enum(ParentState) },
});

pub const Parents = view_mod.Family(26, "workflow.parents", .id, Stored(Parent));
pub const RunFam = view_mod.Family(28, "workflow.run", .id, Stored(Run));
pub const HistFam = view_mod.Family(29, "workflow.hist", .id, Stored(Hist));
pub const IdemFam = view_mod.Family(64, "workflow.idem", .id, Stored(Idem));
pub const PerDefFam = view_mod.Family(65, "workflow.per_def", .id, Stored(PerDef));
pub const OutFam = view_mod.Family(66, "workflow.out", .id, Stored(Out));

const KEY_PREFIX = @sizeOf(u32);

fn typeOf(e: *const Entry) ?EntryType {
    return std.enums.fromInt(EntryType, e.header.entry_type);
}

fn keyNamespace(k: []const u8) NamespaceId {
    return .of(std.mem.readInt(u32, k[0..KEY_PREFIX], .big));
}

// ─── Keys ────────────────────────────────────────────────────────────────────

/// A key under one name (a run, or a workflow): the namespace id, the
/// name's length (u16, big), the name, then `suffix_len` bytes for the
/// caller to fill, so no name's keys are a prefix of another's. One
/// allocation: the caller frees what it returns.
fn underName(a: std.mem.Allocator, ns: NamespaceId, name: []const u8, suffix_len: usize) error{OutOfMemory}![]u8 {
    const b = try a.alloc(u8, KEY_PREFIX + 2 + name.len + suffix_len);
    @memcpy(b[0..KEY_PREFIX], &ns.bytes());
    std.mem.writeInt(u16, b[KEY_PREFIX..][0..2], @intCast(name.len), .big);
    @memcpy(b[KEY_PREFIX + 2 ..][0..name.len], name);
    return b;
}

/// `ns ‖ len ‖ run ‖ seq`, a history event's key.
pub fn histKey(a: std.mem.Allocator, ns: NamespaceId, run: []const u8, seq: u64) error{OutOfMemory}![]u8 {
    const k = try underName(a, ns, run, 8);
    std.mem.writeInt(u64, k[k.len - 8 ..][0..8], seq, .big);
    return k;
}

/// `ns ‖ len ‖ run ‖ step`, a step output's key.
pub fn outKey(a: std.mem.Allocator, ns: NamespaceId, run: []const u8, step: []const u8) error{OutOfMemory}![]u8 {
    const k = try underName(a, ns, run, step.len);
    @memcpy(k[k.len - step.len ..], step);
    return k;
}

/// `ns ‖ len ‖ run`, every key under one run.
pub fn runRange(a: std.mem.Allocator, ns: NamespaceId, run: []const u8) error{OutOfMemory}![]u8 {
    return underName(a, ns, run, 0);
}

/// `ns ‖ len ‖ workflow ‖ idempotency key`.
pub fn idemKey(a: std.mem.Allocator, ns: NamespaceId, workflow: []const u8, key: []const u8) error{OutOfMemory}![]u8 {
    const k = try underName(a, ns, workflow, key.len);
    @memcpy(k[k.len - key.len ..], key);
    return k;
}

fn idKeyAlloc(a: std.mem.Allocator, ns: NamespaceId, k: []const u8) error{OutOfMemory}![]u8 {
    const b = try a.alloc(u8, KEY_PREFIX + k.len);
    return view_mod.idKey(b, ns, k) catch unreachable;
}

// ─── Reads ───────────────────────────────────────────────────────────────────

/// Run `run_key`'s record (namespace id ‖ run id), as applied.
pub fn runOf(read: ReadView, run_key: []const u8) ?Run.Value {
    const got = read.get(RunFam, run_key) catch return null;
    return if (got) |s| s.v else null;
}

/// Child `run`'s parent record in `ns`, as applied.
pub fn parentOf(read: ReadView, ns: NamespaceId, run: []const u8) ?Parent.Value {
    var kb: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    if (run.len > ae.MAX_RUN_ID) return null;
    const k = view_mod.idKey(&kb, ns, run) catch return null;
    const v = (read.get(Parents, k) catch null) orelse return null;
    return v.v;
}

/// The definition `def_key`'s per-workflow record, as applied.
pub fn perDefOf(read: ReadView, def_key: []const u8) ?PerDef.Value {
    const got = read.get(PerDefFam, def_key) catch return null;
    return if (got) |s| s.v else null;
}

// ─── Entry values ────────────────────────────────────────────────────────────

/// A `workflow_start` value:
/// `[wf_len:u16][wf][ver_len:u16][ver][status:u8][evt_len:u16][evt][idem_len:u16][idem][input]`,
/// little-endian lengths. The input is "{}" when none follows.
pub const Start = struct {
    workflow: []const u8,
    version: []const u8,
    status: RunStatus,
    event: []const u8,
    idem: ?[]const u8,
    input: []const u8,

    pub fn parse(value: []const u8) ?Start {
        var c: Cursor = .{ .bytes = value };
        const workflow = c.take(c.int(u16) orelse return null) orelse return null;
        const version = c.take(c.int(u16) orelse return null) orelse return null;
        const status = std.enums.fromInt(RunStatus, (c.take(1) orelse return null)[0]) orelse return null;
        const event = c.take(c.int(u16) orelse return null) orelse return null;
        const idem = c.take(c.int(u16) orelse return null) orelse return null;
        const rest = value[c.off..];
        return .{
            .workflow = workflow,
            .version = version,
            .status = status,
            .event = event,
            .idem = if (idem.len > 0) idem else null,
            .input = if (rest.len > 0) rest else "{}",
        };
    }

    fn isChild(s: *const Start) bool {
        return std.mem.eql(u8, s.event, "child_started");
    }
};

/// A `workflow_complete` value:
///   [status:u8]
///   [has_output:u8]([output_len:u32][output])?
///   [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
///   [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
///   [tags_len:u16][search_tags]
///   [wf_len:u16][workflow]
pub const Complete = struct {
    status: RunStatus,
    output: ?[]const u8,
    step_count: u16,
    steps: Cursor,
    history_count: u16,
    history: Cursor,
    tags: []const u8,
    workflow: []const u8,

    pub fn parse(value: []const u8) ?Complete {
        var c: Cursor = .{ .bytes = value };
        const status = std.enums.fromInt(RunStatus, (c.take(1) orelse return null)[0]) orelse return null;
        if (!status.isTerminal()) return null;
        const has_output = (c.take(1) orelse return null)[0];
        const output: ?[]const u8 = if (has_output == 1) c.take(c.int(u32) orelse return null) orelse return null else null;
        const step_count = c.int(u16) orelse return null;
        const steps = c;
        for (0..step_count) |_| {
            _ = c.take(c.int(u16) orelse return null) orelse return null;
            _ = c.take(c.int(u16) orelse return null) orelse return null;
            _ = c.take(c.int(u32) orelse return null) orelse return null;
        }
        const history_count = c.int(u16) orelse return null;
        const history = c;
        for (0..history_count) |_| {
            _ = c.take(c.int(u16) orelse return null) orelse return null;
            _ = c.take(c.int(u16) orelse return null) orelse return null;
            _ = c.int(i64) orelse return null;
        }
        const tags = c.take(c.int(u16) orelse return null) orelse return null;
        const workflow = c.take(c.int(u16) orelse return null) orelse return null;
        if (c.off != value.len) return null;
        return .{ .status = status, .output = output, .step_count = step_count, .steps = steps, .history_count = history_count, .history = history, .tags = tags, .workflow = workflow };
    }
};

/// How a run ended, for its parent's `call_done`. An output past what a
/// call's result holds goes as none.
pub const Ended = struct { status: ae.DoneStatus, output: []const u8 };

pub fn endOf(c: *const Complete) Ended {
    const status: ae.DoneStatus = switch (c.status) {
        .completed => .completed,
        .cancelled => .cancelled,
        .timed_out => .timed_out,
        else => .failed,
    };
    const out = c.output orelse "";
    return .{ .status = status, .output = if (out.len <= @import("../protocol/action_ops.zig").MAX_OUTPUT) out else "" };
}

/// Reads a value front to back; every read is null rather than past the end.
pub const Cursor = struct {
    bytes: []const u8,
    off: usize = 0,

    pub fn take(self: *Cursor, n: usize) ?[]const u8 {
        if (self.bytes.len - self.off < n) return null;
        defer self.off += n;
        return self.bytes[self.off..][0..n];
    }

    pub fn int(self: *Cursor, comptime T: type) ?T {
        const b = self.take(@sizeOf(T)) orelse return null;
        return std.mem.readInt(T, b[0..@sizeOf(T)], .little);
    }
};

// ─── Keys an entry touches ───────────────────────────────────────────────────

const Named = struct { ns: NamespaceId, run: []const u8 };

/// The run an entry names: a child start or refusal's own fields, or a
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
        .workflow_start, .workflow_step_started, .workflow_complete => blk: {
            const cmd = command(e) orelse return null;
            break :blk .{ .ns = cmd.namespace_id, .run = cmd.key[KEY_PREFIX..] };
        },
        else => null,
    };
}

/// A workflow entry's command, whose key leads with the namespace it is
/// stamped with; null for one that doesn't.
fn command(e: *const Entry) ?entry_mod.CommandPayload {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
    if (cmd.key.len <= KEY_PREFIX or keyNamespace(cmd.key) != cmd.namespace_id) return null;
    return cmd;
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const n = named(e) orelse return;
    if (n.ns == .invalid) return;
    const arena = out.allocator;
    // A child's parent record, keyed like its run; only a child's id fits.
    if (n.run.len <= ae.MAX_RUN_ID) try out.addIn(Parents.ID, n.ns, n.run);
    switch (typeOf(e).?) {
        .workflow_start => {
            const cmd = command(e).?;
            try out.addIn(RunFam.ID, n.ns, n.run);
            const s = Start.parse(cmd.value) orelse return;
            try out.addIn(defs_machine.DefFam.ID, n.ns, s.workflow);
            try out.addIn(PerDefFam.ID, n.ns, s.workflow);
            try out.addIn(HistFam.ID, n.ns, (try histKey(arena, n.ns, n.run, 0))[KEY_PREFIX..]);
            if (s.idem) |k| try out.addIn(IdemFam.ID, n.ns, (try idemKey(arena, n.ns, s.workflow, k))[KEY_PREFIX..]);
        },
        .workflow_step_started => try out.addIn(RunFam.ID, n.ns, n.run),
        .workflow_complete => {
            const cmd = command(e).?;
            try out.addIn(RunFam.ID, n.ns, n.run);
            const range = (try runRange(arena, n.ns, n.run))[KEY_PREFIX..];
            // Not the invalid namespace, checked above.
            out.addRangeIn(HistFam.ID, n.ns, range) catch return error.OutOfMemory;
            out.addRangeIn(OutFam.ID, n.ns, range) catch return error.OutOfMemory;
            const c = Complete.parse(cmd.value) orelse return;
            try out.addIn(PerDefFam.ID, n.ns, c.workflow);
        },
        else => {},
    }
}

/// The namespace a child's start names, held to its origin's.
fn namespaceOf(e: *const Entry) ?u32 {
    if (typeOf(e) != .child_start) return null;
    const n = named(e) orelse return null;
    return n.ns.int();
}

// ─── Apply ───────────────────────────────────────────────────────────────────

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const n = named(e) orelse return error.Halt;
    if (n.ns == .invalid) return error.Halt;
    const stamp_ms: i64 = @intCast(e.header.timestamp_ns / std.time.ns_per_ms);
    switch (typeOf(e).?) {
        .child_start, .child_refuse => return applyParent(view, e, n),
        .workflow_start => return applyStart(view, n, command(e).?, stamp_ms, out),
        .workflow_step_started => {
            const cmd = command(e).?;
            if (cmd.value.len != 0) return error.Halt;
            var run = (try view.get(RunFam, cmd.key)) orelse
                return out.refuse(.conflict, .unclassified, "conflict: the run's first step was taken");
            const ended_steps = comptime plant.active == .runs_ended_takes_step;
            if (!(ended_steps and statusOf(run.v).isTerminal()) and (run.v.stepped != 0 or statusOf(run.v).isTerminal()))
                return out.refuse(.conflict, .unclassified, "conflict: the run's first step was taken");
            run.v.stepped = 1;
            try view.put(RunFam, cmd.key, run);
        },
        .workflow_complete => return applyComplete(view, n, command(e).?, stamp_ms, out),
        else => return error.Halt,
    }
}

fn statusOf(r: Run.Value) RunStatus {
    return std.enums.fromInt(RunStatus, r.status) orelse .failed;
}

fn applyStart(view: *View, n: Named, cmd: entry_mod.CommandPayload, stamp_ms: i64, out: *Result) ApplyError!void {
    const s = Start.parse(cmd.value) orelse return error.Halt;
    const arena = view.arena;

    // Two starts with one idempotency key can both pass the handler's
    // check before either applies; the log's order decides. The later one
    // creates nothing, and its client is answered with the earlier run.
    // Checked before the run id, as the handler does: a retry carrying both
    // its key and its own run id is the same start.
    const idem_key: ?[]const u8 = if (s.idem) |k| try idemKey(arena, n.ns, s.workflow, k) else null;
    if (comptime plant.active != .runs_idem_unchecked) if (idem_key) |k| if (try view.get(IdemFam, k)) |earlier| {
        out.putInt(u8, 0);
        out.putBytes(earlier.v.run);
        return;
    };

    // Already present: a re-apply, or a second start with a run id the
    // client chose, which the handler let through while the first was
    // uncommitted. The log's order decides; the later is refused.
    if (try view.get(RunFam, cmd.key) != null) return out.refuse(.conflict, .unclassified, "conflict: a run with this id already exists");

    // A disable or a redefinition that commits ahead of a start decides
    // it, as the handler's checks would have. Not a trigger's: its leader
    // has moved the trigger's cursor past the record already, so a trigger
    // start proposed before a disable commits still runs. A refused
    // schedule start leaves its slot unfired; enabling re-anchors past it.
    const def_key = try idKeyAlloc(arena, n.ns, s.workflow);
    const def: ?defs_machine.Def.Value = if (try view.get(defs_machine.DefFam, def_key)) |d| d.v else null;
    const client_start = std.mem.eql(u8, s.event, "workflow_started");
    const disabled = if (def) |d| d.enabled == 0 else false;
    if (s.isChild()) {
        if (comptime plant.active == .runs_disabled_child_refused) if (disabled) return out.refuse(.bad_request, .unclassified, "workflow is disabled");
        if (comptime plant.active != .runs_disabled_child_created) if (disabled) return refuseChild(view, n);
    } else if (client_start or std.mem.eql(u8, s.event, "schedule_started")) {
        if (disabled) return out.refuse(.bad_request, .unclassified, "workflow is disabled");
        if (client_start and s.idem == null) if (def) |d| {
            if (d.idempotency == @backingInt(@import("definition.zig").IdempotencyMode.required))
                return out.refuse(.bad_request, .unclassified, "idempotency key is required for this workflow");
        };
    }

    var run: Run.Value = .{
        .workflow = s.workflow,
        .version = s.version,
        .input = s.input,
        .idempotency_key = s.idem orelse "",
        .status = @backingInt(s.status),
        .created_at_ms = stamp_ms,
        .completed_at_ms = 0,
        // Only a running run has a first step to take.
        .stepped = @intFromBool(s.status != .running),
        .has_output = 0,
        .output = "",
        .search_tags = if (def) |d| try searchTags(arena, d.yaml, s.input, n.run) else "",
        .hist_next = 0,
        .hist_dropped = 0,
    };
    // The run's first history event, as the start path recorded it.
    if (s.event.len > 0) {
        try view.put(HistFam, try histKey(arena, n.ns, n.run, 0), .{ .v = .{ .event = s.event, .detail = s.input, .at_ms = stamp_ms } });
        run.hist_next = 1;
    }
    try view.put(RunFam, cmd.key, .{ .v = run });
    if (comptime plant.active != .runs_idem_unwritten) if (idem_key) |k| try view.put(IdemFam, k, .{ .v = .{ .run = n.run } });

    var per = if (try view.get(PerDefFam, def_key)) |p| p.v else PerDef.Value{ .active = 0, .fired_ms = 0 };
    if (!s.status.isTerminal()) per.active +|= 1;
    if (s.idem) |k| noteFired(&per, s.event, k);
    try view.put(PerDefFam, def_key, .{ .v = per });

    // A child's parent hears it started. Only the child's own start: one
    // of another kind holding its id leaves the record pending, and this
    // shard's leader refuses the child, a conflict.
    var pk: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    if ((s.isChild() or comptime plant.active == .runs_any_start_answers_parent) and n.run.len <= ae.MAX_RUN_ID) {
        const k = view_mod.idKey(&pk, n.ns, n.run) catch unreachable;
        if (try view.get(Parents, k)) |found| if (found.v.state == .pending) {
            var p = found;
            p.v.state = .started;
            try view.put(Parents, k, p);
            try reply(view, n.ns, n.run, p.v.parent_slot, .started, null);
        };
    }

    out.putInt(u8, 1);
    out.putBytes(n.run);
}

/// A schedule's start fired its slot, the key's last part. It never moves
/// back.
fn noteFired(per: *PerDef.Value, event: []const u8, idem: []const u8) void {
    const any_fires = comptime plant.active == .runs_any_start_fires;
    if ((!any_fires and !std.mem.eql(u8, event, "schedule_started")) or !std.mem.startsWith(u8, idem, "schedule:")) return;
    const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
    const slot = std.fmt.parseInt(i64, idem[colon + 1 ..], 10) catch return;
    per.fired_ms = if (comptime plant.active == .runs_fired_moves_back) slot else @max(per.fired_ms, slot);
}

/// A disabled child's start: no run, and its parent's call is refused,
/// saying why. The entry itself isn't refused, which would drop the reply.
fn refuseChild(view: *View, n: Named) ApplyError!void {
    var pk: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    if (n.run.len > ae.MAX_RUN_ID) return;
    const k = view_mod.idKey(&pk, n.ns, n.run) catch unreachable;
    const p = (try view.get(Parents, k)) orelse return;
    if (p.v.state != .pending) return;
    try reply(view, n.ns, n.run, p.v.parent_slot, .refused, "workflow is disabled");
    try view.delete(Parents, k);
}

fn applyComplete(view: *View, n: Named, cmd: entry_mod.CommandPayload, stamp_ms: i64, out: *Result) ApplyError!void {
    const c = Complete.parse(cmd.value) orelse return error.Halt;
    const arena = view.arena;
    // Runs are never removed: a completion names one that started.
    var run = ((try view.get(RunFam, cmd.key)) orelse return error.Halt).v;
    if (!std.mem.eql(u8, run.workflow, c.workflow)) return error.Halt;
    // The first completion in the log ends the run; a cancel and a
    // completion in flight together leave it as the first left it. A second
    // with the same outcome (two cancels) succeeds, changing nothing.
    if (statusOf(run).isTerminal()) {
        if (statusOf(run) == c.status) return;
        return out.refuse(.bad_request, .unclassified, "run already finished");
    }

    run.status = @backingInt(c.status);
    run.completed_at_ms = stamp_ms;
    run.stepped = 1;
    run.has_output = @intFromBool(c.output != null);
    run.output = c.output orelse "";
    if (c.tags.len > 0) run.search_tags = c.tags;

    // The entry carries the run's history and step outputs as its leader
    // kept them: they replace what was there.
    const range = try runRange(arena, n.ns, n.run);
    try view.deleteRange(HistFam, range, null);
    const keep_from: u64 = c.history_count -| (if (comptime plant.active == .runs_hist_keeps_257) HIST_KEEP + 1 else HIST_KEEP);
    var h = c.history;
    for (0..c.history_count) |i| {
        const event = h.take(h.int(u16).?).?;
        const detail = h.take(h.int(u16).?).?;
        const at_ms = h.int(i64).?;
        if (i < keep_from) continue;
        try view.put(HistFam, try histKey(arena, n.ns, n.run, i), .{ .v = .{ .event = event, .detail = detail, .at_ms = at_ms } });
    }
    run.hist_next = c.history_count;
    run.hist_dropped = keep_from;

    try view.deleteRange(OutFam, range, null);
    var st = c.steps;
    for (0..c.step_count) |_| {
        const name = st.take(st.int(u16).?).?;
        const outcome = st.take(st.int(u16).?).?;
        const output = st.take(st.int(u32).?).?;
        try view.put(OutFam, try outKey(arena, n.ns, n.run, name), .{ .v = .{ .outcome = outcome, .output = output } });
    }
    try view.put(RunFam, cmd.key, .{ .v = run });

    const def_key = try idKeyAlloc(arena, n.ns, c.workflow);
    if (try view.get(PerDefFam, def_key)) |found| {
        var per = found;
        const cancel_keeps = comptime plant.active == .runs_cancel_keeps_active;
        if (!cancel_keeps or c.status != .cancelled) per.v.active -|= 1;
        try view.put(PerDefFam, def_key, per);
    }

    // A child's parent hears how it ended, and its record goes.
    var pk: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    if (n.run.len > ae.MAX_RUN_ID) return;
    const k = view_mod.idKey(&pk, n.ns, n.run) catch unreachable;
    const p = (try view.get(Parents, k)) orelse return;
    const ended = endOf(&c);
    const buf = try arena.alloc(u8, ae.Done.max_size);
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
}

/// A child's start from its parent's call, or this shard's refusal of it.
fn applyParent(view: *View, e: *const Entry, n: Named) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    var kb: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    if (n.run.len > ae.MAX_RUN_ID) return error.Halt;
    const k = view_mod.idKey(&kb, n.ns, n.run) catch unreachable;
    const found = try view.get(Parents, k);
    switch (typeOf(e).?) {
        .child_start => {
            const p = ae.ChildStart.decodeAll(e.payload, &diag) catch return error.Halt;
            if (found) |f| {
                // Sent again for the same call: the first stands. Another
                // caller's start naming the same run (an id repeated across
                // leaders) is answered so, and its step fails.
                if (f.v.parent_slot == p.parent_slot and std.mem.eql(u8, f.v.parent_run, p.parent_run)) return;
                return reply(view, n.ns, n.run, p.parent_slot, .conflict, null);
            }
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
            const p = ae.ChildRefuse.decodeAll(e.payload, &diag) catch return error.Halt;
            // Refused by an earlier leader already, or started meanwhile.
            const f = found orelse return;
            if (f.v.state != .pending) return;
            try reply(view, n.ns, n.run, f.v.parent_slot, p.answer, null);
            try view.delete(Parents, k);
        },
        else => return error.Halt,
    }
}

fn reply(view: *View, ns: NamespaceId, run: []const u8, parent_slot: u16, answer: ae.Answer, why: ?[]const u8) ApplyError!void {
    var buf: [ae.Reply.max_size]u8 = undefined;
    const bytes = ae.Reply.encodeInto(&buf, .{ .ns = ns.int(), .slot = parent_slot, .run = run, .answer = answer, .why = if (why) |w| w[0..@min(w.len, ae.MAX_SKIP_WHY)] else null }) catch return error.Halt;
    try view.send(.user(ns), parent_slot, @backingInt(EntryType.call_reply), bytes);
}

/// A child's start a repair skipped: its parent's call is told, so the step
/// fails saying so.
fn skipped(view: *View, e: *const Entry, why: []const u8) ApplyError!void {
    if (typeOf(e) != .child_start) return;
    var diag: schema.Diagnostic = .{};
    const p = ae.ChildStart.decodeAll(e.payload, &diag) catch return;
    try reply(view, .of(p.ns), p.run, p.parent_slot, .skipped, why);
}

/// The run's search attributes, from its definition and its input:
/// `{"name":value,...}`, a value null where its path resolves to nothing.
/// Empty when the definition names none.
fn searchTags(arena: std.mem.Allocator, yaml: []const u8, input: []const u8, run_id: []const u8) error{OutOfMemory}![]const u8 {
    var def = parser.parseWorkflow(arena, yaml, null) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        // Its create parsed it here already.
        return "";
    };
    defer def.deinit(arena);
    if (def.search_attributes.len == 0) return "";
    var resolver = jsonpath.PathResolver.init(arena, input, null, run_id);
    var aw: std.Io.Writer.Allocating = .init(arena);
    const w = &aw.writer;
    w.writeByte('{') catch return error.OutOfMemory;
    for (def.search_attributes, 0..) |attr, i| {
        const resolved = resolver.resolve(attr.from) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => null,
        };
        if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
        w.writeByte('"') catch return error.OutOfMemory;
        writeJsonEscaped(w, attr.name) catch return error.OutOfMemory;
        w.writeAll("\":") catch return error.OutOfMemory;
        // Already JSON: strings quoted, numbers raw.
        w.writeAll(resolved orelse "null") catch return error.OutOfMemory;
    }
    w.writeByte('}') catch return error.OutOfMemory;
    return aw.written();
}

fn writeJsonEscaped(w: *std.Io.Writer, s: []const u8) !void {
    for (s) |ch| switch (ch) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => if (ch < 0x20) try w.print("\\u{x:0>4}", .{ch}) else try w.writeByte(ch),
    };
}

pub const machine: machine_mod.Machine = .{
    .name = "workflow runs",
    .entry_types = &.{ .workflow_start, .workflow_step_started, .workflow_complete, .child_start, .child_refuse },
    .families = &.{
        machine_mod.info(Parents),
        machine_mod.info(RunFam),
        machine_mod.info(HistFam),
        machine_mod.info(IdemFam),
        machine_mod.info(PerDefFam),
        machine_mod.info(OutFam),
    },
    .keys = keys,
    .apply = apply,
    .namespace = namespaceOf,
    .skipped = skipped,
};

// ─── Tests ───────────────────────────────────────────────────────────────────

const testing = std.testing;
const Store = @import("../contract/store.zig").Store;
const channels = @import("../contract/channels.zig");

const Fixture = struct {
    store: Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init() Fixture {
        var f: Fixture = .{ .store = Store.init(testing.allocator), .scratch = .init(testing.allocator) };
        f.store.strict_keys = true;
        // A group of two, so a parent's slot is another group's.
        f.store.place = .{ .group = 0, .count = 2 };
        return f;
    }

    fn deinit(f: *Fixture) void {
        f.store.deinit();
        f.scratch.deinit();
    }

    fn run(m: *const machine_mod.Machine, f: *Fixture, e: *const Entry) !Result {
        var out = Result.start(e);
        try machine_mod.applyEntry(m, &f.store, &f.scratch, e, &out);
        return out;
    }

    fn command(f: *Fixture, t: EntryType, key: []const u8, value: []const u8, stamp_ms: u64) !Result {
        var pbuf: [4096]u8 = undefined;
        f.index += 1;
        const e = entry_mod.buildCommandEntry(t, entry_mod.Flags.NONE, 1, f.index, stamp_ms * std.time.ns_per_ms, .default, key, value, &pbuf).?;
        return run(if (t == .workflow_create or t == .workflow_enabled) &defs_machine.machine else &machine, f, &e);
    }

    fn start(f: *Fixture, run_id: []const u8, wf: []const u8, event: []const u8, idem: []const u8, stamp_ms: u64) !Result {
        var kb: [64]u8 = undefined;
        var vb: [512]u8 = undefined;
        return f.command(.workflow_start, view_mod.idKey(&kb, .default, run_id) catch unreachable, startValue(&vb, wf, event, idem), stamp_ms);
    }

    fn complete(f: *Fixture, run_id: []const u8, value: []const u8, stamp_ms: u64) !Result {
        var kb: [64]u8 = undefined;
        return f.command(.workflow_complete, view_mod.idKey(&kb, .default, run_id) catch unreachable, value, stamp_ms);
    }

    fn stepStarted(f: *Fixture, run_id: []const u8) !Result {
        var kb: [64]u8 = undefined;
        return f.command(.workflow_step_started, view_mod.idKey(&kb, .default, run_id) catch unreachable, "", 1);
    }

    fn create(f: *Fixture, wf: []const u8) !void {
        var kb: [64]u8 = undefined;
        var yb: [256]u8 = undefined;
        const yaml = std.fmt.bufPrint(&yb, "kind: Workflow\nname: {s}\nversion: 1.0.0\nstart:\n  wait_for_signal:\n    type: never\n  transitions:\n    success: flo.Completed\n", .{wf}) catch unreachable;
        _ = try f.command(.workflow_create, view_mod.idKey(&kb, .default, wf) catch unreachable, yaml, 1);
    }

    fn disable(f: *Fixture, wf: []const u8) !void {
        var kb: [64]u8 = undefined;
        _ = try f.command(.workflow_enabled, view_mod.idKey(&kb, .default, wf) catch unreachable, &.{0}, 2);
    }

    fn get(f: *Fixture, run_id: []const u8) ?Run.Value {
        var kb: [64]u8 = undefined;
        return runOf(.{ .store = &f.store }, view_mod.idKey(&kb, .default, run_id) catch unreachable);
    }

    fn active(f: *Fixture, wf: []const u8) ?PerDef.Value {
        var kb: [64]u8 = undefined;
        return perDefOf(.{ .store = &f.store }, view_mod.idKey(&kb, .default, wf) catch unreachable);
    }

    /// The seqs of `run_id`'s history, oldest first.
    fn histSeqs(f: *Fixture, run_id: []const u8, out: []u64) ![]u64 {
        const prefix = try runRange(testing.allocator, .default, run_id);
        defer testing.allocator.free(prefix);
        var it = (ReadView{ .store = &f.store }).range(HistFam, prefix);
        var n: usize = 0;
        while (try it.next()) |item| : (n += 1) out[n] = std.mem.readInt(u64, item.key[item.key.len - 8 ..][0..8], .big);
        return out[0..n];
    }
};

fn startValue(buf: []u8, wf: []const u8, event: []const u8, idem: []const u8) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.writeInt(u16, @intCast(wf.len), .little) catch unreachable;
    w.writeAll(wf) catch unreachable;
    w.writeInt(u16, 1, .little) catch unreachable;
    w.writeAll("1") catch unreachable;
    w.writeByte(@backingInt(RunStatus.running)) catch unreachable;
    w.writeInt(u16, @intCast(event.len), .little) catch unreachable;
    w.writeAll(event) catch unreachable;
    w.writeInt(u16, @intCast(idem.len), .little) catch unreachable;
    w.writeAll(idem) catch unreachable;
    return w.buffered();
}

/// A completion's value with `events` history events, each "e"/"" at its
/// own seq's ms.
fn completeValue(buf: []u8, status: RunStatus, wf: []const u8, events: u16) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.writeByte(@backingInt(status)) catch unreachable;
    w.writeByte(1) catch unreachable;
    w.writeInt(u32, 2, .little) catch unreachable;
    w.writeAll("ok") catch unreachable;
    // One step: "s", "done", "x".
    w.writeInt(u16, 1, .little) catch unreachable;
    for ([_][]const u8{ "s", "done" }) |p| {
        w.writeInt(u16, @intCast(p.len), .little) catch unreachable;
        w.writeAll(p) catch unreachable;
    }
    w.writeInt(u32, 1, .little) catch unreachable;
    w.writeAll("x") catch unreachable;
    w.writeInt(u16, events, .little) catch unreachable;
    for (0..events) |i| {
        w.writeInt(u16, 1, .little) catch unreachable;
        w.writeAll("e") catch unreachable;
        w.writeInt(u16, 0, .little) catch unreachable;
        w.writeInt(i64, @intCast(i), .little) catch unreachable;
    }
    w.writeInt(u16, 0, .little) catch unreachable;
    w.writeInt(u16, @intCast(wf.len), .little) catch unreachable;
    w.writeAll(wf) catch unreachable;
    return w.buffered();
}

test "workflow runs machine: a start creates its run, first event, idempotency key and count; one with a used key or id creates nothing" {
    var f = Fixture.init();
    defer f.deinit();
    var r = try f.start("r1", "wf", "workflow_started", "k", 1000);
    var rd = r.reader();
    try testing.expectEqual(@as(?u8, 1), rd.int(u8));
    const run = f.get("r1").?;
    try testing.expectEqualStrings("wf", run.workflow);
    try testing.expectEqual(@as(i64, 1000), run.created_at_ms);
    try testing.expectEqual(@as(u8, 0), run.stepped);
    try testing.expectEqual(@as(u64, 1), run.hist_next);
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);
    var seqs: [4]u64 = undefined;
    try testing.expectEqualSlices(u64, &.{0}, try f.histSeqs("r1", &seqs));

    // The key started r1: a second start with it is answered with r1.
    r = try f.start("r2", "wf", "workflow_started", "k", 1001);
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 0), rd.int(u8));
    try testing.expectEqualStrings("r1", rd.slice().?);
    try testing.expect(f.get("r2") == null);
    // A second start of r1 is refused.
    r = try f.start("r1", "wf", "workflow_started", "", 1002);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.conflict, r.refused().?.status);
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);
}

test "workflow runs machine: a run's first step is taken once, and never after it ends" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    try testing.expect((try f.stepStarted("r1")).refused() == null);
    try testing.expectEqual(@as(u8, 1), f.get("r1").?.stepped);
    try testing.expect((try f.stepStarted("r1")).refused() != null);
    try testing.expect((try f.stepStarted("nope")).refused() != null);

    _ = try f.start("r2", "wf", "workflow_started", "", 1);
    var vb: [512]u8 = undefined;
    _ = try f.complete("r2", completeValue(&vb, .cancelled, "wf", 1), 2);
    try testing.expect((try f.stepStarted("r2")).refused() != null);
}

test "workflow runs machine: a completion ends its run with its logged history, keeping the last 256, and its step outputs; a second with the same status changes nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    _ = try f.start("r2", "wf", "workflow_started", "", 1);
    try testing.expectEqual(@as(u32, 2), f.active("wf").?.active);

    const buf = try testing.allocator.alloc(u8, 16 * 1024);
    defer testing.allocator.free(buf);
    const value = completeValue(buf, .completed, "wf", 300);
    try testing.expect((try f.complete("r1", value, 50)).refused() == null);
    const run = f.get("r1").?;
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.completed)), run.status);
    try testing.expectEqual(@as(i64, 50), run.completed_at_ms);
    try testing.expectEqualStrings("ok", run.output);
    try testing.expectEqual(@as(u64, 300), run.hist_next);
    try testing.expectEqual(@as(u64, 300 - HIST_KEEP), run.hist_dropped);
    var seqs: [300]u64 = undefined;
    const kept = try f.histSeqs("r1", &seqs);
    try testing.expectEqual(@as(usize, HIST_KEEP), kept.len);
    try testing.expectEqual(@as(u64, 300 - HIST_KEEP), kept[0]);
    try testing.expectEqual(@as(u64, 299), kept[kept.len - 1]);
    // The other run is still counted.
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);

    // A second completion, the same status: OK, and nothing changes.
    const commits = f.store.families[RunFam.ID].commits;
    try testing.expect((try f.complete("r1", value, 60)).refused() == null);
    try testing.expectEqual(commits, f.store.families[RunFam.ID].commits);
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);
    // Another status: refused.
    var vb: [512]u8 = undefined;
    const other = try f.complete("r1", completeValue(&vb, .cancelled, "wf", 1), 70);
    try testing.expectEqualStrings("run already finished", other.refused().?.refusal.message);
    // One naming another workflow, or a run never started, is a broken log.
    try testing.expectError(error.Halt, f.complete("r2", completeValue(&vb, .completed, "other", 1), 80));
    try testing.expectError(error.Halt, f.complete("nope", completeValue(&vb, .completed, "wf", 1), 80));
}

test "workflow runs machine: a disabled workflow refuses a client or schedule start, not a trigger's, and refuses a child to its parent without creating it" {
    var f = Fixture.init();
    defer f.deinit();
    try f.create("wf");
    try f.disable("wf");
    var r = try f.start("c", "wf", "workflow_started", "", 3);
    try testing.expectEqualStrings("workflow is disabled", r.refused().?.refusal.message);
    r = try f.start("s", "wf", "schedule_started", "schedule:16:wf:4000", 3);
    try testing.expect(r.refused() != null);
    try testing.expect(f.active("wf") == null);
    r = try f.start("t", "wf", "trigger_started", "trigger:ev@5:2", 3);
    try testing.expect(r.refused() == null);
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);

    // A parent's call for child "kid": its record, then its start.
    var cb: [ae.ChildStart.max_size]u8 = undefined;
    const cs = try ae.ChildStart.encodeInto(&cb, .{ .ns = NamespaceId.default.int(), .parent_slot = 7, .parent_run = "par", .parent_workflow = "pwf", .workflow = "wf", .version = "", .run = "kid", .input = "{}" });
    f.index += 1;
    const ce = entry_mod.buildEntry(.child_start, entry_mod.Flags.NONE, 1, f.index, 0, cs);
    try testing.expect((try Fixture.run(&machine, &f, &ce)).refused() == null);
    try testing.expect(parentOf(.{ .store = &f.store }, .default, "kid") != null);
    const sent = f.store.families[channels.OutFam.ID].count;
    r = try f.start("kid", "wf", "child_started", "", 4);
    // Not refused: the parent's reply leaves with it.
    try testing.expect(r.refused() == null);
    try testing.expect(f.get("kid") == null);
    try testing.expect(parentOf(.{ .store = &f.store }, .default, "kid") == null);
    try testing.expectEqual(sent + 1, f.store.families[channels.OutFam.ID].count);
    var it = (ReadView{ .store = &f.store }).range(channels.OutFam, "");
    var told: ?ae.Reply.Value = null;
    while (try it.next()) |item| {
        if (item.value.v.entry_type != @backingInt(EntryType.call_reply)) continue;
        var diag: schema.Diagnostic = .{};
        told = try ae.Reply.decodeAll(item.value.v.payload, &diag);
    }
    try testing.expectEqual(ae.Answer.refused, told.?.answer);
    try testing.expectEqualStrings("workflow is disabled", told.?.why.?);
}

test "workflow runs machine: a schedule's fired slot never moves back" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("a", "wf", "schedule_started", "schedule:16:wf:4000", 1);
    _ = try f.start("b", "wf", "schedule_started", "schedule:16:wf:2000", 1);
    try testing.expectEqual(@as(i64, 4000), f.active("wf").?.fired_ms);
    // A client's start with a key of that shape fires nothing.
    _ = try f.start("c", "wf", "workflow_started", "schedule:16:wf:9000", 1);
    try testing.expectEqual(@as(i64, 4000), f.active("wf").?.fired_ms);
}
