//! The workflow runs machine: each run as its log made it, and a child
//! run's parent.
//!
//! - `workflow.run`, keyed (namespace id, run id): what started it, how it
//!   stands, and, once it ended, how and with what output.
//! - its history: none here. Each apply that adds events puts them in the
//!   run's pending chunk (`hist_pending`), sent on the history lane to the
//!   run's own slot as one `history_chunk` once full, and once the run
//!   ends; the stream machine keeps where each chunk is in the log
//!   (`stream/history_machine.zig`).
//! - `workflow.out`, each step's outcome and output, by step name.
//! - `workflow.idem`: the run each (workflow, idempotency key) started, so
//!   a later start with that key is answered with it.
//! - `workflow.per_def`, per workflow: its runs not yet ended (what a
//!   schedule's `max_concurrent` counts), the last schedule slot a start
//!   fired, and its trigger's place: the stream (namespace id, name, the
//!   stamp that made it) and the last event a trigger's start applied from.
//!   A place in another stream is replaced, so a redefinition onto another
//!   stream, or a stream made again, reads that stream from its start.
//! - `workflow.parents`: a child run's parent, from the parent's call
//!   (`child_start`) until the child ends.
//! - `step.calls`: each step's call of an action's run or a child's, kept
//!   by `actions/calls_machine.zig`, whose entries this machine applies
//!   there. A step's call is recorded, and its start sent, in the apply of
//!   the `workflow_step` that makes it.
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
const he = @import("../protocol/history_entries.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const defs_machine = @import("defs_machine.zig");
const calls_mod = @import("../actions/calls_machine.zig");
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

/// The most event bytes a run holds before sending them: a chunk. An event
/// over half of it goes at once, with those before it.
pub const HIST_CHUNK: usize = 4 << 10;

/// How much of an ended run's output its record keeps; the whole is in its
/// history, as `OUTPUT_EVENT`'s detail, when it is longer.
pub const OUTPUT_PREVIEW: usize = 1024;
pub const OUTPUT_EVENT = "workflow_output";

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
    .{ .name = "has_output", .T = Int(u8) },
    // Once it ended, only its first `OUTPUT_PREVIEW` bytes, and whether
    // there were more: the whole is its history's `OUTPUT_EVENT`.
    .{ .name = "output", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "output_truncated", .T = schema.Bool },
    // Empty once it ended, as its input is: a search covers live runs.
    .{ .name = "search_tags", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // Its history: the seq the next event takes, the first not yet sent,
    // and those since, encoded (`HistRec`), sent once a chunk is full or
    // the run ends.
    .{ .name = "hist_next", .T = Int(u64) },
    .{ .name = "hist_flushed", .T = Int(u64) },
    .{ .name = "hist_pending", .T = Bytes(HIST_CHUNK) },
    // The stamp of the entry that started it: its history's incarnation,
    // so a run id used again never reads an earlier run's.
    .{ .name = "created_ns", .T = Int(u64) },
    // The step it is at ("" for its start), and how many steps it has
    // taken: a `workflow_step` applies only at this `rev`, then adds one.
    .{ .name = "step", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "rev", .T = Int(u64) },
    // What it waits for (`Wait`), and its fields: a call's kind, the
    // callee's slot and run id; a signal's type, its deadline and where it
    // goes then; a timer's due stamp.
    .{ .name = "wait", .T = schema.Enum(Wait) },
    .{ .name = "wait_kind", .T = schema.Enum(ae.CallKind) },
    .{ .name = "wait_slot", .T = Int(u16) },
    .{ .name = "wait_on", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "wait_due_ms", .T = Int(i64) },
    .{ .name = "wait_timeout_to", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // The step's own counters, as its leader encoded them; kept verbatim.
    .{ .name = "progress", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // The seq the next signal takes (`workflow.sig`), and how many it holds
    // that no wait has taken: those `MAX_SIGNALS` bounds.
    .{ .name = "sig_next", .T = Int(u64) },
    .{ .name = "sig_held", .T = Int(u64) },
});

/// The most signals one run holds that no wait has taken; one past it is
/// refused. A wait takes the oldest it holds of its type, so each signal
/// ends one wait.
pub const MAX_SIGNALS: u64 = 64;

/// A `workflow_signal` value: a client's signal to the run its key names.
pub const Signal = Record(&.{
    .{ .name = "type", .T = Bytes(MAX_NAME) },
    .{ .name = "payload", .T = Bytes(MAX_BODY) },
});

/// A signal a run was sent, at its entry's stamp.
/// A signal type a step's wait took: the oldest of it the run holds.
pub const Take = Record(&.{
    .{ .name = "type", .T = Bytes(MAX_NAME) },
});

pub const Sig = Record(&.{
    .{ .name = "type", .T = Bytes(MAX_NAME) },
    .{ .name = "payload", .T = Bytes(MAX_BODY) },
    .{ .name = "at_ms", .T = Int(i64) },
});

/// What a run waits for between steps.
pub const Wait = enum(u8) {
    /// Nothing: its leader takes its next step.
    none = 0,
    /// A step's call: an action's run or a child's (`step.calls`).
    call = 1,
    /// A signal of `wait_on`'s type, until `wait_due_ms` (0: no deadline).
    signal = 2,
    /// Until `wait_due_ms`: a poll, a retry, or a hold for room.
    timer = 3,
};

/// The most a step's counters (`progress`) may hold.
pub const MAX_PROGRESS: usize = 1024;

/// The most a logged event's type, a step's name or an outcome holds.
const MAX_NAME = 1024;
/// The most a step's event detail or output holds: an entry, less the
/// rest of the step.
const MAX_BODY = MAX_PERSIST_PAYLOAD - 4 * MAX_NAME;

pub const Event = Record(&.{
    .{ .name = "event", .T = Bytes(MAX_NAME) },
    .{ .name = "detail", .T = Bytes(MAX_BODY) },
    .{ .name = "at_ms", .T = Int(i64) },
});

pub const StepOutput = Record(&.{
    .{ .name = "step", .T = Bytes(MAX_NAME) },
    .{ .name = "outcome", .T = Bytes(MAX_NAME) },
    .{ .name = "output", .T = Bytes(MAX_BODY) },
});

/// The events and outputs one step may log.
pub const MAX_STEP_EVENTS: u32 = 64;
pub const MAX_STEP_OUTPUTS: u32 = 16;

/// A `workflow_step` value: the step its leader took, decided from the run
/// at `rev`. Its key is the run's.
pub const Step = Record(&.{
    .{ .name = "rev", .T = Int(u64) },
    .{ .name = "step", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "events", .T = schema.BoundedList(Event, MAX_STEP_EVENTS, MAX_PERSIST_PAYLOAD) },
    .{ .name = "outputs", .T = schema.BoundedList(StepOutput, MAX_STEP_OUTPUTS, MAX_PERSIST_PAYLOAD) },
    .{ .name = "wait", .T = schema.Enum(Wait) },
    .{ .name = "wait_kind", .T = schema.Enum(ae.CallKind) },
    .{ .name = "wait_slot", .T = Int(u16) },
    .{ .name = "wait_on", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "wait_due_ms", .T = Int(i64) },
    .{ .name = "wait_timeout_to", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "progress", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // The callee whose end the step takes, when it takes its run's call's
    // end (empty for none): the run's call, and settled in `step.calls`, at
    // the run's workflow's slot (`caller_slot`).
    .{ .name = "settles", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "caller_slot", .T = Int(u16) },
    // The run's workflow, which counts its runs (`workflow.per_def`).
    .{ .name = "workflow", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // When the step ends the run: how (a terminal `RunStatus`; 0 for a step
    // that doesn't) and its output.
    .{ .name = "ends", .T = Int(u8) },
    .{ .name = "has_output", .T = schema.Bool },
    .{ .name = "output", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // The call the step makes (an `ae.Call`; empty for none): recorded in
    // `step.calls` and its start sent in this apply, its callee the run's
    // new wait.
    .{ .name = "call", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // The signals its waits took, in order: each the oldest of its type the
    // run holds, gone from `workflow.sig` in this apply.
    .{ .name = "takes", .T = schema.BoundedList(Take, MAX_SIGNALS, MAX_PERSIST_PAYLOAD) },
});

/// One event of a run's history, as its chunk carries it.
pub const HistRec = Record(&.{
    .{ .name = "seq", .T = Int(u64) },
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
    // Its trigger's place: the stream a trigger's start last read (its
    // namespace id, its made stamp and name; 0 and "" for none), and the
    // latest event in it a start applied from.
    .{ .name = "trig_ns", .T = Int(u32) },
    .{ .name = "trig_made", .T = Int(u64) },
    .{ .name = "trig_stream", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "trig_ts", .T = Int(u64) },
    .{ .name = "trig_seq", .T = Int(u64) },
});

pub const PER_DEF_NONE: PerDef.Value = .{ .active = 0, .fired_ms = 0, .trig_ns = 0, .trig_made = 0, .trig_stream = "", .trig_ts = 0, .trig_seq = 0 };

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
pub const IdemFam = view_mod.Family(64, "workflow.idem", .id, Stored(Idem));
pub const PerDefFam = view_mod.Family(65, "workflow.per_def", .id, Stored(PerDef));
pub const OutFam = view_mod.Family(66, "workflow.out", .id, Stored(Out));
pub const SigFam = view_mod.Family(67, "workflow.sig", .id, Stored(Sig));

/// `ns ‖ len ‖ run ‖ seq`, a signal's key.
pub fn sigKey(a: std.mem.Allocator, ns: NamespaceId, run: []const u8, seq: u64) error{OutOfMemory}![]u8 {
    return histKey(a, ns, run, seq);
}

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
/// `[wf_len:u16][wf][ver_len:u16][ver][status:u8][evt_len:u16][evt][idem_len:u16][idem]`
/// `[has_trigger:u8]([ns:u32][made:u64][stream_len:u16][stream][ts:u64][seq:u64])?[input]`,
/// little-endian. The input is "{}" when none follows.
pub const Start = struct {
    workflow: []const u8,
    version: []const u8,
    status: RunStatus,
    event: []const u8,
    idem: ?[]const u8,
    /// A trigger's start: the stream its poll read, and the event (a
    /// batch's last) the run starts from.
    trigger: ?TriggerAt = null,
    input: []const u8,

    pub fn parse(value: []const u8) ?Start {
        var c: Cursor = .{ .bytes = value };
        const workflow = c.take(c.int(u16) orelse return null) orelse return null;
        const version = c.take(c.int(u16) orelse return null) orelse return null;
        const status = std.enums.fromInt(RunStatus, (c.take(1) orelse return null)[0]) orelse return null;
        const event = c.take(c.int(u16) orelse return null) orelse return null;
        const idem = c.take(c.int(u16) orelse return null) orelse return null;
        const has_trigger = (c.take(1) orelse return null)[0];
        const trigger: ?TriggerAt = switch (has_trigger) {
            0 => null,
            1 => .{
                .ns = c.int(u32) orelse return null,
                .made = c.int(u64) orelse return null,
                .stream = c.take(c.int(u16) orelse return null) orelse return null,
                .ts = c.int(u64) orelse return null,
                .seq = c.int(u64) orelse return null,
            },
            else => return null,
        };
        const rest = value[c.off..];
        return .{
            .workflow = workflow,
            .version = version,
            .status = status,
            .event = event,
            .idem = if (idem.len > 0) idem else null,
            .trigger = trigger,
            .input = if (rest.len > 0) rest else "{}",
        };
    }

    /// The value into `buf`; null when it doesn't fit, or a length doesn't
    /// fit its u16.
    pub fn encode(s: Start, buf: []u8) ?[]const u8 {
        var w: std.Io.Writer = .fixed(buf);
        for ([_][]const u8{ s.workflow, s.version }) |p| {
            if (p.len > std.math.maxInt(u16)) return null;
            w.writeInt(u16, @intCast(p.len), .little) catch return null;
            w.writeAll(p) catch return null;
        }
        w.writeByte(@backingInt(s.status)) catch return null;
        for ([_][]const u8{ s.event, s.idem orelse "" }) |p| {
            if (p.len > std.math.maxInt(u16)) return null;
            w.writeInt(u16, @intCast(p.len), .little) catch return null;
            w.writeAll(p) catch return null;
        }
        if (s.trigger) |t| {
            if (t.stream.len > std.math.maxInt(u16)) return null;
            w.writeByte(1) catch return null;
            w.writeInt(u32, t.ns, .little) catch return null;
            w.writeInt(u64, t.made, .little) catch return null;
            w.writeInt(u16, @intCast(t.stream.len), .little) catch return null;
            w.writeAll(t.stream) catch return null;
            w.writeInt(u64, t.ts, .little) catch return null;
            w.writeInt(u64, t.seq, .little) catch return null;
        } else w.writeByte(0) catch return null;
        w.writeAll(s.input) catch return null;
        return w.buffered();
    }

    fn isChild(s: *const Start) bool {
        return std.mem.eql(u8, s.event, "child_started");
    }
};

/// Where a trigger's start read: the stream, and the event.
pub const TriggerAt = struct {
    ns: u32,
    made: u64,
    stream: []const u8,
    ts: u64,
    seq: u64,
};

/// Whether an idempotency key is the server's: a leading '_', as for
/// signal types. A schedule's start keys its slot ("_schedule:..."), a
/// trigger's its event ("_trigger:...").
pub fn serverKey(key: []const u8) bool {
    return key.len > 0 and key[0] == '_';
}

/// Whether a start of `event` may carry a server's key: a schedule's or a
/// trigger's, which the server makes. Decided by the start's event, never
/// by the key.
fn carriesServerKeys(event: []const u8) bool {
    return std.mem.eql(u8, event, "schedule_started") or std.mem.eql(u8, event, "trigger_started");
}

pub const SERVER_KEY_REFUSED = "reserved idempotency key: a leading '_' is the server's";

/// A `workflow_complete` value:
///   [status:u8]
///   [has_output:u8]([output_len:u32][output])?
///   [event_len:u16][event][detail_len:u16][detail]
///   [wf_len:u16][workflow]
/// The event is the run's last, stamped with the entry's time.
pub const Complete = struct {
    status: RunStatus,
    output: ?[]const u8,
    event: []const u8,
    detail: []const u8,
    workflow: []const u8,

    pub fn parse(value: []const u8) ?Complete {
        var c: Cursor = .{ .bytes = value };
        const status = std.enums.fromInt(RunStatus, (c.take(1) orelse return null)[0]) orelse return null;
        if (!status.isTerminal()) return null;
        const has_output = (c.take(1) orelse return null)[0];
        if (has_output > 1) return null;
        const output: ?[]const u8 = if (has_output == 1) c.take(c.int(u32) orelse return null) orelse return null else null;
        const event = c.take(c.int(u16) orelse return null) orelse return null;
        const detail = c.take(c.int(u16) orelse return null) orelse return null;
        const workflow = c.take(c.int(u16) orelse return null) orelse return null;
        if (c.off != value.len) return null;
        return .{ .status = status, .output = output, .event = event, .detail = detail, .workflow = workflow };
    }

    /// Its encoding into `buf`, or null when it doesn't fit.
    pub fn encode(c: Complete, buf: []u8) ?[]const u8 {
        var w: std.Io.Writer = .fixed(buf);
        w.writeByte(@backingInt(c.status)) catch return null;
        if (c.output) |o| {
            w.writeByte(1) catch return null;
            w.writeInt(u32, std.math.cast(u32, o.len) orelse return null, .little) catch return null;
            w.writeAll(o) catch return null;
        } else w.writeByte(0) catch return null;
        for ([_][]const u8{ c.event, c.detail, c.workflow }) |part| {
            w.writeInt(u16, std.math.cast(u16, part.len) orelse return null, .little) catch return null;
            w.writeAll(part) catch return null;
        }
        return w.buffered();
    }

    pub fn size(c: Complete) usize {
        return 2 + (if (c.output) |o| 4 + o.len else 0) + 6 + c.event.len + c.detail.len + c.workflow.len;
    }
};

/// How a run ended, for its parent's `call_done`. An output past what a
/// call's result holds goes as none.
pub const Ended = struct { status: ae.DoneStatus, output: []const u8 };

pub fn endOf(status: RunStatus, output: ?[]const u8) Ended {
    const done: ae.DoneStatus = switch (status) {
        .completed => .completed,
        .cancelled => .cancelled,
        .timed_out => .timed_out,
        else => .failed,
    };
    const out = output orelse "";
    return .{ .status = done, .output = if (out.len <= @import("../protocol/action_ops.zig").MAX_OUTPUT) out else "" };
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
        .workflow_start, .workflow_step, .workflow_signal, .workflow_complete => blk: {
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
    if (calls_mod.owns(typeOf(e) orelse return)) return calls_mod.keys(e, out);
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
            if (s.idem) |k| try out.addIn(IdemFam.ID, n.ns, (try idemKey(arena, n.ns, s.workflow, k))[KEY_PREFIX..]);
        },
        .workflow_step => {
            const cmd = command(e).?;
            try out.addIn(RunFam.ID, n.ns, n.run);
            const range = (try runRange(arena, n.ns, n.run))[KEY_PREFIX..];
            out.addRangeIn(OutFam.ID, n.ns, range) catch return error.OutOfMemory;
            // A wait on a signal: whether the run holds one of its type.
            out.addRangeIn(SigFam.ID, n.ns, range) catch return error.OutOfMemory;
            var diag: schema.Diagnostic = .{};
            const st = Step.decodeAll(cmd.value, &diag) catch return;
            if (st.settles.len > 0) if (try calls_mod.callKeyAlloc(arena, n.ns, st.caller_slot, st.settles)) |k| try out.addIn(calls_mod.Calls.ID, n.ns, k[KEY_PREFIX..]);
            try out.addIn(PerDefFam.ID, n.ns, st.workflow);
            if (st.call.len > 0) {
                const p = ae.Call.decodeAll(st.call, &diag) catch return;
                if (try calls_mod.recordKey(arena, p)) |k| try out.addIn(calls_mod.Calls.ID, n.ns, k[KEY_PREFIX..]);
            }
        },
        .workflow_signal => {
            try out.addIn(RunFam.ID, n.ns, n.run);
            const range = (try runRange(arena, n.ns, n.run))[KEY_PREFIX..];
            out.addRangeIn(SigFam.ID, n.ns, range) catch return error.OutOfMemory;
        },
        .workflow_complete => {
            const cmd = command(e).?;
            try out.addIn(RunFam.ID, n.ns, n.run);
            const range = (try runRange(arena, n.ns, n.run))[KEY_PREFIX..];
            // Not the invalid namespace, checked above.
            out.addRangeIn(OutFam.ID, n.ns, range) catch return error.OutOfMemory;
            // An ended run's signals go with it.
            out.addRangeIn(SigFam.ID, n.ns, range) catch return error.OutOfMemory;
            const c = Complete.parse(cmd.value) orelse return;
            try out.addIn(PerDefFam.ID, n.ns, c.workflow);
        },
        else => {},
    }
}

/// The namespace a child's start names, held to its origin's.
fn namespaceOf(e: *const Entry) ?u32 {
    if (calls_mod.owns(typeOf(e) orelse return null)) return calls_mod.namespaceOf(e);
    if (typeOf(e) != .child_start) return null;
    const n = named(e) orelse return null;
    return n.ns.int();
}

// ─── Apply ───────────────────────────────────────────────────────────────────

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    if (calls_mod.owns(typeOf(e) orelse return error.Halt)) return calls_mod.apply(view, e, out);
    const n = named(e) orelse return error.Halt;
    if (n.ns == .invalid) return error.Halt;
    const stamp_ms: i64 = @intCast(e.header.timestamp_ns / std.time.ns_per_ms);
    switch (typeOf(e).?) {
        .child_start, .child_refuse => return applyParent(view, e, n),
        .workflow_start => return applyStart(view, n, command(e).?, e.header.timestamp_ns, out),
        .workflow_step => return applyStep(view, n, command(e).?, stamp_ms, out),
        .workflow_signal => return applySignal(view, n, command(e).?, stamp_ms, out),
        .workflow_complete => return applyComplete(view, n, command(e).?, stamp_ms, out),
        else => return error.Halt,
    }
}

fn statusOf(r: Run.Value) RunStatus {
    return std.enums.fromInt(RunStatus, r.status) orelse .failed;
}

fn applyStart(view: *View, n: Named, cmd: entry_mod.CommandPayload, stamp_ns: u64, out: *Result) ApplyError!void {
    const stamp_ms: i64 = @intCast(stamp_ns / std.time.ns_per_ms);
    const s = Start.parse(cmd.value) orelse return error.Halt;
    const arena = view.arena;
    // Only a trigger's start says where it read, and every one does.
    if ((s.trigger != null) != std.mem.eql(u8, s.event, "trigger_started")) return error.Halt;
    // A server's key on a start the server didn't make: refused.
    if (s.idem) |k| if (serverKey(k)) if (comptime plant.active != .runs_reserved_key_taken) if (!carriesServerKeys(s.event))
        return out.refuse(.bad_request, .unclassified, SERVER_KEY_REFUSED);

    // Two starts with one idempotency key can both pass the handler's
    // check before either applies; the log's order decides. The later one
    // creates nothing, and its client is answered with the earlier run.
    // Checked before the run id, as the handler does: a retry carrying both
    // its key and its own run id is the same start.
    const idem_key: ?[]const u8 = if (s.idem) |k| try idemKey(arena, n.ns, s.workflow, k) else null;
    if (comptime plant.active != .runs_idem_unchecked) if (idem_key) |k| if (try view.get(IdemFam, k)) |earlier| {
        // A trigger's start read again: its event is still read.
        if (s.trigger) |t| {
            const def_key = try idKeyAlloc(arena, n.ns, s.workflow);
            var per = if (try view.get(PerDefFam, def_key)) |p| p.v else PER_DEF_NONE;
            notePlace(&per, t);
            try view.put(PerDefFam, def_key, .{ .v = per });
        }
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
        .has_output = 0,
        .output = "",
        .output_truncated = false,
        .search_tags = if (def) |d| try searchTags(arena, d.yaml, s.input, n.run) else "",
        .hist_next = 0,
        .hist_flushed = 0,
        .hist_pending = "",
        .created_ns = stamp_ns,
        .sig_next = 0,
        .sig_held = 0,
        .step = "",
        .rev = 0,
        .wait = .none,
        .wait_kind = .action,
        .wait_slot = 0,
        .wait_on = "",
        .wait_due_ms = 0,
        .wait_timeout_to = "",
        .progress = "",
    };
    // The run's first history event, as the start path recorded it. A run
    // that starts ended sends it as its last.
    if (s.event.len > 0) try appendHist(view, n, &run, s.event, s.input, stamp_ms);
    if (s.status.isTerminal()) {
        run.completed_at_ms = stamp_ms;
        try summarize(view, n, &run, null, false, stamp_ms);
    }
    try view.put(RunFam, cmd.key, .{ .v = run });
    if (comptime plant.active != .runs_idem_unwritten) if (idem_key) |k| try view.put(IdemFam, k, .{ .v = .{ .run = n.run } });

    var per = if (try view.get(PerDefFam, def_key)) |p| p.v else PER_DEF_NONE;
    if (!s.status.isTerminal()) per.active +|= 1;
    if (s.idem) |k| noteFired(&per, s.event, k);
    if (s.trigger) |t| notePlace(&per, t);
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
    if ((!any_fires and !std.mem.eql(u8, event, "schedule_started")) or !std.mem.startsWith(u8, idem, "_schedule:")) return;
    const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
    const slot = std.fmt.parseInt(i64, idem[colon + 1 ..], 10) catch return;
    per.fired_ms = if (comptime plant.active == .runs_fired_moves_back) slot else @max(per.fired_ms, slot);
}

/// A trigger's start moves its place: on in the stream it read, or to that
/// stream when it is another. Made stamps rise in one stream's log, so a
/// start read from an older making of the stream (applied after a newer
/// one's) moves nothing.
fn notePlace(per: *PerDef.Value, t: TriggerAt) void {
    const same_stream = per.trig_ns == t.ns and std.mem.eql(u8, per.trig_stream, t.stream);
    if (same_stream and comptime plant.active != .runs_trigger_made_unchecked) {
        if (t.made < per.trig_made) return;
        if (t.made == per.trig_made) {
            const later = t.ts > per.trig_ts or (t.ts == per.trig_ts and t.seq > per.trig_seq);
            if (later or comptime plant.active == .runs_trigger_place_moves_back) {
                per.trig_ts = t.ts;
                per.trig_seq = t.seq;
            }
            return;
        }
    }
    per.* = .{ .active = per.active, .fired_ms = per.fired_ms, .trig_ns = t.ns, .trig_made = t.made, .trig_stream = t.stream, .trig_ts = t.ts, .trig_seq = t.seq };
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

/// A client's signal: kept, said in its run's history, and, when the run
/// waits on a signal of its type, the wake: the run is runnable, and its
/// `rev` moves on, so a step decided from that wait (its timeout) is
/// refused rather than also moving it.
fn applySignal(view: *View, n: Named, cmd: entry_mod.CommandPayload, stamp_ms: i64, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    // Its handler encoded it: one that doesn't decode is a Flo bug.
    const sig = Signal.decodeAll(cmd.value, &diag) catch return error.Halt;
    var run = ((try view.get(RunFam, cmd.key)) orelse return out.refuse(.not_found, .not_found, "")).v;
    if (comptime plant.active != .runs_signal_after_end) if (statusOf(run).isTerminal()) return out.refuse(.bad_request, .unclassified, "run already finished");
    const held = if (comptime plant.active == .runs_signal_bound_counts_all) run.sig_next else run.sig_held;
    if (held >= MAX_SIGNALS) return out.refuse(.bad_request, .unclassified, "run's signals are full");
    try view.put(SigFam, try sigKey(view.arena, n.ns, n.run, run.sig_next), .{ .v = .{ .type = sig.type, .payload = sig.payload, .at_ms = stamp_ms } });
    run.sig_next += 1;
    run.sig_held += 1;
    try appendHist(view, n, &run, "signal_received", sig.type, stamp_ms);
    const any_type = comptime plant.active == .runs_wake_any_type;
    if (run.wait == .signal and (any_type or std.mem.eql(u8, run.wait_on, sig.type))) {
        try appendHist(view, n, &run, "signal_matched", sig.type, stamp_ms);
        run.wait = .none;
        run.wait_on = "";
        run.wait_due_ms = 0;
        run.wait_timeout_to = "";
        run.status = @backingInt(RunStatus.running);
        if (comptime plant.active != .runs_signal_wake_keeps_rev) run.rev += 1;
    }
    try view.put(RunFam, cmd.key, .{ .v = run });
}

/// Whether run `run_key` holds a signal of `signal_type` no wait has
/// taken, as applied.
pub fn hasSignal(a: std.mem.Allocator, read: ReadView, run_key: []const u8, signal_type: []const u8) error{ OutOfMemory, Malformed }!bool {
    return try signalsHeld(a, read, run_key, signal_type) > 0;
}

/// How many signals of `signal_type` run `run_key` holds that no wait has
/// taken, as applied.
pub fn signalsHeld(a: std.mem.Allocator, read: ReadView, run_key: []const u8, signal_type: []const u8) error{ OutOfMemory, Malformed }!u64 {
    const prefix = try runRange(a, keyNamespace(run_key), run_key[KEY_PREFIX..]);
    defer a.free(prefix);
    var it = read.range(SigFam, prefix);
    var n: u64 = 0;
    while (try it.next()) |item| {
        if (std.mem.eql(u8, item.value.v.type, signal_type)) n += 1;
    }
    return n;
}

/// The key of the oldest signal of `signal_type` run `n` holds, past those
/// in `taken`, if any.
fn oldestHeld(view: *View, n: Named, signal_type: []const u8, taken: []const []const u8) ApplyError!?[]const u8 {
    var it = try view.range(SigFam, try runRange(view.arena, n.ns, n.run));
    var newest: ?[]const u8 = null;
    next: while (try it.next()) |item| {
        if (!std.mem.eql(u8, item.value.v.type, signal_type)) continue;
        if (comptime plant.active != .runs_take_ignores_picked) for (taken) |k| if (std.mem.eql(u8, k, item.key)) continue :next;
        if (comptime plant.active == .runs_take_newest) {
            newest = try view.arena.dupe(u8, item.key);
            continue;
        }
        return item.key;
    }
    return newest;
}

/// One event onto `run`'s history at its next seq, in its pending chunk:
/// sent first when it wouldn't fit there, and with it when it is over
/// half a chunk.
fn appendHist(view: *View, n: Named, run: *Run.Value, event: []const u8, detail: []const u8, at_ms: i64) ApplyError!void {
    const a = view.arena;
    const rec: HistRec.Value = .{ .seq = run.hist_next, .event = event, .detail = detail, .at_ms = at_ms };
    const buf = try a.alloc(u8, HistRec.encodedSize(rec) catch return error.Halt);
    var w: schema.Writer = .{ .buf = buf };
    HistRec.encode(&w, rec) catch return error.Halt;
    const bytes = w.written();
    if (run.hist_pending.len + bytes.len > HIST_CHUNK and run.hist_pending.len > 0) try flushHist(view, n, run, null);
    const joined = try a.alloc(u8, run.hist_pending.len + bytes.len);
    @memcpy(joined[0..run.hist_pending.len], run.hist_pending);
    @memcpy(joined[run.hist_pending.len..], bytes);
    run.hist_pending = joined;
    run.hist_next += 1;
    if (bytes.len > HIST_CHUNK / 2) try flushHist(view, n, run, null);
}

/// An ended run's record made its summary: its output's preview, the whole
/// in its history when longer; no input, tags or step outputs, all of
/// which its history holds or a search of live runs needed; and its last
/// chunk sent. A run that starts ended has no step outputs to drop.
fn summarize(view: *View, n: Named, run: *Run.Value, output: ?[]const u8, has_steps: bool, stamp_ms: i64) ApplyError!void {
    const out = output orelse "";
    run.has_output = @intFromBool(output != null);
    run.output_truncated = out.len > OUTPUT_PREVIEW;
    if (run.output_truncated) try appendHist(view, n, run, OUTPUT_EVENT, out, stamp_ms);
    run.output = out[0..@min(out.len, OUTPUT_PREVIEW)];
    run.input = "";
    run.search_tags = "";
    if (has_steps and comptime plant.active != .runs_end_keeps_outputs) try view.deleteRange(OutFam, try runRange(view.arena, n.ns, n.run), null);
    try flushHist(view, n, run, stamp_ms);
}

/// `run`'s pending events sent as one chunk on the history lane, to its
/// workflow's slot, which is the run's own; with `ended_ms` the last, the
/// run having ended then. A last chunk goes even with no events, so the
/// run's rows move to its one ended row.
fn flushHist(view: *View, n: Named, run: *Run.Value, ended_ms: ?i64) ApplyError!void {
    const final = ended_ms != null;
    if (run.hist_pending.len == 0 and !final) return;
    const a = view.arena;
    // A run's events are its own seqs: none pending is an empty range at
    // the next one.
    const last = if (run.hist_next > run.hist_flushed) run.hist_next - 1 else run.hist_flushed;
    const buf = try a.alloc(u8, he.Chunk.max_size);
    const payload = he.Chunk.encodeInto(buf, .{
        .ns = n.ns.int(),
        .family = @backingInt(he.Family.workflow),
        .owner = run.workflow,
        .run = n.run,
        .created_ns = run.created_ns,
        .first_seq = run.hist_flushed,
        .last_seq = last,
        .final = final,
        .completed_ms = if (ended_ms) |ms| @intCast(@max(ms, 0)) else 0,
        .records = run.hist_pending,
    }) catch return error.Halt;
    try view.sendFlagged(.user(n.ns), calls_mod.callerSlot(n.ns, run.workflow), @backingInt(EntryType.history_chunk), entry_mod.Flags.HISTORY, payload);
    if (comptime plant.active != .runs_hist_flushed_kept) run.hist_flushed = run.hist_next;
    run.hist_pending = "";
}

/// A step its leader took from the run at `rev`: applied only while the
/// run is still there, so two proposals from one state, by any leaders,
/// move it once. One that takes the run's call's end applies only once
/// that call is settled.
fn applyStep(view: *View, n: Named, cmd: entry_mod.CommandPayload, stamp_ms: i64, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    // Only a leader proposes one: one that doesn't decode is a Flo bug.
    const st = Step.decodeAll(cmd.value, &diag) catch return error.Halt;
    var run = ((try view.get(RunFam, cmd.key)) orelse return out.refuse(.not_found, .not_found, "run not found")).v;
    if (!std.mem.eql(u8, run.workflow, st.workflow)) return error.Halt;
    const ends: ?RunStatus = if (st.ends == 0) null else blk: {
        const status = std.enums.fromInt(RunStatus, st.ends) orelse return error.Halt;
        if (!status.isTerminal()) return error.Halt;
        break :blk status;
    };
    const ended_steps = comptime plant.active == .runs_ended_takes_step;
    if (!ended_steps and statusOf(run).isTerminal()) return out.refuse(.conflict, .unclassified, "conflict: the run has ended");
    if (comptime plant.active != .runs_step_rev_unchecked) if (st.rev != run.rev) return out.refuse(.conflict, .unclassified, "conflict: the run moved on from that step");
    // Each wait it passed takes the oldest signal of its type the run holds.
    var taken: std.ArrayListUnmanaged([]const u8) = .empty;
    var takes = st.takes.iterator();
    while (takes.next() catch return error.Halt) |t| {
        const k = (try oldestHeld(view, n, t.type, taken.items)) orelse return out.refuse(.conflict, .unclassified, "conflict: the run holds no such signal");
        try taken.append(view.arena, try view.arena.dupe(u8, k));
    }
    // A signal of its type applied after the wait was decided and before
    // it: the run would wait on what it holds, which wakes nothing. Refused,
    // so its next step is decided with the signal there.
    if (comptime plant.active != .runs_stale_wait_unchecked) if (st.wait == .signal and try oldestHeld(view, n, st.wait_on, taken.items) != null) return out.refuse(.conflict, .unclassified, "conflict: the run holds that signal already");
    if (st.progress.len > MAX_PROGRESS) return out.refuse(.bad_request, .unclassified, "a step's counters are past their bound");
    // A step leaves its run's call only by taking its end, or by ending the
    // run (the call then goes with the run, unanswered).
    const leaves_call = run.wait == .call and !(st.wait == .call and std.mem.eql(u8, st.wait_on, run.wait_on));
    if (comptime plant.active != .runs_call_left_unsettled) if (leaves_call and ends == null and st.settles.len == 0) return out.refuse(.conflict, .unclassified, "conflict: the step leaves its run's call unsettled");
    if (st.settles.len > 0) {
        if (run.wait != .call or !std.mem.eql(u8, run.wait_on, st.settles)) return out.refuse(.conflict, .unclassified, "conflict: the run waits on another call");
        if (st.caller_slot != calls_mod.callerSlot(n.ns, run.workflow)) return error.Halt;
        // Its record goes with the step that takes its end. One already
        // gone (cleared with a repair) can't be unfinished: taken as lost.
        const k = (try calls_mod.callKeyAlloc(view.arena, n.ns, st.caller_slot, st.settles)) orelse return error.Halt;
        if (try view.get(calls_mod.Calls, k)) |call| {
            switch (call.v.state) {
                .done, .refused, .no_action, .over_budget => {},
                .sent, .started => if (comptime plant.active != .runs_settle_before_call_ends) return out.refuse(.conflict, .unclassified, "conflict: the run's call hasn't ended"),
            }
            if (comptime plant.active != .runs_settled_call_kept) try view.delete(calls_mod.Calls, k);
        }
    }

    if (comptime plant.active != .runs_take_keeps_signal) for (taken.items) |k| try view.delete(SigFam, k);
    run.sig_held -= @intCast(taken.items.len);

    if (st.call.len > 0) {
        const p = ae.Call.decodeAll(st.call, &diag) catch return error.Halt;
        // The call is the run's new wait, of this run, and from its workflow.
        if (st.wait != .call or !std.mem.eql(u8, p.run, st.wait_on) or p.ns != n.ns.int() or
            !std.mem.eql(u8, p.caller_run, n.run) or !std.mem.eql(u8, p.caller_workflow, run.workflow)) return error.Halt;
        try calls_mod.record(view, p, @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms);
    }
    var events = st.events.iterator();
    while (events.next() catch return error.Halt) |ev| try appendHist(view, n, &run, ev.event, ev.detail, ev.at_ms);
    var outputs = st.outputs.iterator();
    while (outputs.next() catch return error.Halt) |o| {
        try view.put(OutFam, try outKey(view.arena, n.ns, n.run, o.step), .{ .v = .{ .outcome = o.outcome, .output = o.output } });
    }
    run.rev += 1;
    run.step = st.step;
    run.wait = st.wait;
    run.wait_kind = st.wait_kind;
    run.wait_slot = st.wait_slot;
    run.wait_on = st.wait_on;
    run.wait_due_ms = st.wait_due_ms;
    run.wait_timeout_to = st.wait_timeout_to;
    run.progress = st.progress;
    run.status = @backingInt(if (st.wait == .none) RunStatus.running else RunStatus.waiting);
    if (ends) |status| return endRun(view, n, cmd.key, &run, status, if (st.has_output) st.output else null, stamp_ms);
    try view.put(RunFam, cmd.key, .{ .v = run });
}

fn applyComplete(view: *View, n: Named, cmd: entry_mod.CommandPayload, stamp_ms: i64, out: *Result) ApplyError!void {
    const c = Complete.parse(cmd.value) orelse return error.Halt;
    // A run deleted since its completion was proposed: refused, as a step
    // or a signal to it is.
    var run = ((try view.get(RunFam, cmd.key)) orelse return out.refuse(.not_found, .not_found, "run not found")).v;
    if (!std.mem.eql(u8, run.workflow, c.workflow)) return error.Halt;
    // The first completion in the log ends the run; a cancel and a
    // completion in flight together leave it as the first left it. A second
    // with the same outcome (two cancels) succeeds, changing nothing.
    if (statusOf(run).isTerminal()) {
        if (statusOf(run) == c.status) return;
        return out.refuse(.bad_request, .unclassified, "run already finished");
    }

    try appendHist(view, n, &run, c.event, c.detail, stamp_ms);
    return endRun(view, n, cmd.key, &run, c.status, c.output, stamp_ms);
}

/// `run` ended `status` at `stamp_ms`: no longer counted among its
/// workflow's, and a child's parent hears how, its record gone.
fn endRun(view: *View, n: Named, run_key: []const u8, run: *Run.Value, status: RunStatus, output: ?[]const u8, stamp_ms: i64) ApplyError!void {
    const arena = view.arena;
    run.status = @backingInt(status);
    run.completed_at_ms = stamp_ms;
    run.wait = .none;
    run.wait_kind = .action;
    run.wait_slot = 0;
    run.wait_on = "";
    run.wait_due_ms = 0;
    run.wait_timeout_to = "";
    run.progress = "";
    try summarize(view, n, run, output, true, stamp_ms);
    // Signals no wait took: none can now.
    if (comptime plant.active != .runs_end_keeps_signals) {
        try view.deleteRange(SigFam, try runRange(arena, n.ns, n.run), null);
        run.sig_held = 0;
    }
    try view.put(RunFam, run_key, .{ .v = run.* });

    const def_key = try idKeyAlloc(arena, n.ns, run.workflow);
    if (try view.get(PerDefFam, def_key)) |found| {
        var per = found;
        const cancel_keeps = comptime plant.active == .runs_cancel_keeps_active;
        if (!cancel_keeps or status != .cancelled) per.v.active -|= 1;
        try view.put(PerDefFam, def_key, per);
    }

    var pk: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    if (n.run.len > ae.MAX_RUN_ID) return;
    const k = view_mod.idKey(&pk, n.ns, n.run) catch unreachable;
    const p = (try view.get(Parents, k)) orelse return;
    const ended = endOf(status, output);
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
    if (calls_mod.owns(typeOf(e) orelse return)) return calls_mod.skipped(view, e, why);
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
    .entry_types = &.{ .workflow_start, .workflow_step, .workflow_signal, .workflow_complete, .child_start, .child_refuse, .step_call_retry, .step_call_clear, .call_reply, .call_done },
    .families = &.{
        machine_mod.info(calls_mod.Calls),
        machine_mod.info(Parents),
        machine_mod.info(RunFam),
        machine_mod.info(IdemFam),
        machine_mod.info(PerDefFam),
        machine_mod.info(OutFam),
        machine_mod.info(SigFam),
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
        // A group of two, so a parent's slot is another group's; this one
        // owns workflow "wf"'s slot, where its runs' history goes.
        const group = channels.ownerOf(.{ .group = 0, .count = 2 }, calls_mod.callerSlot(.default, "wf"));
        f.store.place = .{ .group = group, .count = 2 };
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

    fn step(f: *Fixture, run_id: []const u8, spec: StepSpec) !Result {
        var kb: [64]u8 = undefined;
        var events_buf: [16 * 1024]u8 = undefined;
        var ew: schema.Writer = .{ .buf = &events_buf };
        for (0..spec.events) |i| try Event.encode(&ew, .{ .event = "e", .detail = "", .at_ms = @intCast(i) });
        var outputs_buf: [256]u8 = undefined;
        var ow: schema.Writer = .{ .buf = &outputs_buf };
        if (spec.output_step.len > 0) try StepOutput.encode(&ow, .{ .step = spec.output_step, .outcome = "success", .output = "{}" });
        var takes_buf: [256]u8 = undefined;
        var tw: schema.Writer = .{ .buf = &takes_buf };
        for (spec.takes) |t| try Take.encode(&tw, .{ .type = t });
        const value: Step.Value = .{
            .rev = spec.rev,
            .step = spec.at,
            .events = .{ .bytes = ew.written(), .count = spec.events },
            .outputs = .{ .bytes = ow.written(), .count = @intFromBool(spec.output_step.len > 0) },
            .wait = spec.wait,
            .wait_kind = .action,
            .wait_slot = 0,
            .wait_on = spec.wait_on,
            .wait_due_ms = 0,
            .wait_timeout_to = "",
            .progress = spec.progress,
            .settles = spec.settles,
            .caller_slot = calls_mod.callerSlot(.default, spec.workflow),
            .workflow = spec.workflow,
            .ends = if (spec.ends) |e| @backingInt(e) else 0,
            .has_output = spec.ends != null,
            .output = if (spec.ends != null) "out" else "",
            .call = spec.call,
            .takes = .{ .bytes = tw.written(), .count = @intCast(spec.takes.len) },
        };
        var vb: [32 * 1024]u8 = undefined;
        return f.command(.workflow_step, view_mod.idKey(&kb, .default, run_id) catch unreachable, try Step.encodeInto(&vb, value), 1);
    }

    /// A call's reply or end, as the callee's shard sends it.
    fn answer(f: *Fixture, t: EntryType, payload: []const u8) !Result {
        f.index += 1;
        const e = entry_mod.buildEntry(t, entry_mod.Flags.NONE, 1, f.index, 0, payload);
        return run(&machine, f, &e);
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

    /// The history chunks this group has sent itself, oldest first, as
    /// they are in its outbox (into `f.scratch`).
    fn chunks(f: *Fixture) ![]he.Chunk.Value {
        const a = f.scratch.allocator();
        var pb: [channels.CHAN_KEY]u8 = undefined;
        const prefix = channels.outPrefix(&pb, f.store.place.?.group, .history);
        var it = (ReadView{ .store = &f.store }).range(channels.OutFam, prefix);
        var got: std.ArrayListUnmanaged(he.Chunk.Value) = .empty;
        while (try it.next()) |item| {
            var diag: schema.Diagnostic = .{};
            try got.append(a, try he.Chunk.decodeAll(try a.dupe(u8, item.value.v.payload), &diag));
        }
        return got.items;
    }

    /// The seqs `records` (a chunk's, or a run's pending) holds, in order.
    fn seqsOf(records: []const u8, out: []u64) ![]u64 {
        var diag: schema.Diagnostic = .{};
        var r: schema.Reader = .{ .bytes = records, .diag = &diag };
        var n: usize = 0;
        while (r.pos < records.len) : (n += 1) out[n] = (try HistRec.decode(&r, "rec")).seq;
        return out[0..n];
    }
};

const StepSpec = struct {
    rev: u64,
    at: []const u8 = "s",
    events: u32 = 1,
    wait: Wait = .none,
    wait_on: []const u8 = "",
    settles: []const u8 = "",
    progress: []const u8 = "",
    ends: ?RunStatus = null,
    call: []const u8 = "",
    workflow: []const u8 = "wf",
    output_step: []const u8 = "",
    takes: []const []const u8 = &.{},
};

fn startValue(buf: []u8, wf: []const u8, event: []const u8, idem: []const u8) []const u8 {
    // A trigger's start says where it read.
    const at: ?TriggerAt = if (std.mem.eql(u8, event, "trigger_started")) .{ .ns = NamespaceId.default.int(), .made = 1, .stream = "ev", .ts = 1, .seq = 0 } else null;
    return (Start{ .workflow = wf, .version = "1", .status = .running, .event = event, .idem = if (idem.len > 0) idem else null, .trigger = at, .input = "" }).encode(buf).?;
}

/// A cancel's completion of a run of `wf`.
fn completeValue(buf: []u8, status: RunStatus, wf: []const u8) []const u8 {
    const c: Complete = .{ .status = status, .output = null, .event = "workflow_cancelled", .detail = "stop", .workflow = wf };
    return c.encode(buf).?;
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
    try testing.expectEqual(@as(u64, 0), run.rev);
    try testing.expectEqual(@as(u64, 1), run.hist_next);
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);
    // Its first event is pending, under its incarnation's stamp.
    var seqs: [4]u64 = undefined;
    try testing.expectEqualSlices(u64, &.{0}, try Fixture.seqsOf(run.hist_pending, &seqs));
    try testing.expectEqual(@as(u64, 1000 * std.time.ns_per_ms), run.created_ns);
    try testing.expectEqual(@as(usize, 0), (try f.chunks()).len);

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

test "workflow runs machine: a step applies only at its run's rev, and never after the run ends" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    try testing.expect((try f.step("r1", .{ .rev = 0, .at = "a" })).refused() == null);
    var run = f.get("r1").?;
    try testing.expectEqual(@as(u64, 1), run.rev);
    try testing.expectEqualStrings("a", run.step);
    // Two leaders' steps from one state: only the first applies.
    const stale = try f.step("r1", .{ .rev = 0, .at = "b" });
    try testing.expectEqualStrings("conflict: the run moved on from that step", stale.refused().?.refusal.message);
    try testing.expectEqualStrings("a", f.get("r1").?.step);
    try testing.expect((try f.step("nope", .{ .rev = 0 })).refused() != null);
    // One naming another workflow is a broken log.
    try testing.expectError(error.Halt, f.step("r1", .{ .rev = 1, .workflow = "other" }));

    // A step that ends its run: counted no longer, and no step after it.
    try testing.expect((try f.step("r1", .{ .rev = 1, .ends = .completed })).refused() == null);
    run = f.get("r1").?;
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.completed)), run.status);
    try testing.expectEqualStrings("out", run.output);
    try testing.expectEqual(@as(u32, 0), f.active("wf").?.active);
    try testing.expect((try f.step("r1", .{ .rev = 2 })).refused() != null);
}

test "workflow runs machine: a run's history is sent in chunks, each event once and in order, the last with its end; a cancel ends it and a second cancel changes nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    _ = try f.start("r2", "wf", "workflow_started", "", 1);
    // 1 from the start, then 5 steps of 60.
    for (0..5) |i| try testing.expect((try f.step("r1", .{ .rev = i, .events = 60 })).refused() == null);
    var run = f.get("r1").?;
    try testing.expectEqual(@as(u64, 301), run.hist_next);
    try testing.expect(run.hist_pending.len <= HIST_CHUNK);
    // What was sent and what is pending: every seq, once, in order.
    var seqs: [400]u64 = undefined;
    var n: usize = 0;
    for (try f.chunks()) |c| {
        try testing.expectEqualStrings("r1", c.run);
        try testing.expectEqualStrings("wf", c.owner);
        try testing.expect(!c.final);
        try testing.expect(c.records.len <= HIST_CHUNK);
        const got = try Fixture.seqsOf(c.records, seqs[n..]);
        try testing.expectEqual(c.first_seq, got[0]);
        try testing.expectEqual(c.last_seq, got[got.len - 1]);
        n += got.len;
    }
    try testing.expect(n > 0);
    try testing.expectEqual(@as(u64, n), run.hist_flushed);
    n += (try Fixture.seqsOf(run.hist_pending, seqs[n..])).len;
    for (seqs[0..n], 0..) |seq, i| try testing.expectEqual(@as(u64, i), seq);
    try testing.expectEqual(@as(usize, 301), n);

    var vb: [512]u8 = undefined;
    try testing.expect((try f.complete("r1", completeValue(&vb, .cancelled, "wf"), 50)).refused() == null);
    run = f.get("r1").?;
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.cancelled)), run.status);
    try testing.expectEqual(@as(i64, 50), run.completed_at_ms);
    try testing.expectEqual(@as(u64, 302), run.hist_next);
    // Its end sent what was pending as its last chunk.
    try testing.expectEqual(@as(u64, 302), run.hist_flushed);
    try testing.expectEqualStrings("", run.hist_pending);
    const sent = try f.chunks();
    const last = sent[sent.len - 1];
    try testing.expect(last.final);
    try testing.expectEqual(@as(u64, 50), last.completed_ms);
    try testing.expectEqual(@as(u64, 301), last.last_seq);
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);
    // A second cancel: OK, and nothing changes. Another end: refused.
    const commits = f.store.families[RunFam.ID].commits;
    try testing.expect((try f.complete("r1", completeValue(&vb, .cancelled, "wf"), 60)).refused() == null);
    try testing.expectEqual(commits, f.store.families[RunFam.ID].commits);
    const other = try f.complete("r1", completeValue(&vb, .failed, "wf"), 70);
    try testing.expectEqualStrings("run already finished", other.refused().?.refusal.message);
    // One naming another workflow is a broken log; one for a run not
    // there (deleted since it was proposed) is refused, changing nothing.
    try testing.expectError(error.Halt, f.complete("r2", completeValue(&vb, .cancelled, "other"), 80));
    const gone = try f.complete("nope", completeValue(&vb, .cancelled, "wf"), 80);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.not_found, gone.refused().?.status);
    try testing.expect(f.get("nope") == null);
}

test "workflow runs machine: a step, a signal or a completion for a run not there is refused, sending nothing" {
    var f = Fixture.init();
    defer f.deinit();
    var vb: [512]u8 = undefined;
    const sent = (try f.chunks()).len;
    try testing.expect((try f.step("gone", .{ .rev = 0 })).refused() != null);
    try testing.expect((try f.complete("gone", completeValue(&vb, .completed, "wf"), 5)).refused() != null);
    var kb: [64]u8 = undefined;
    var sb: [64]u8 = undefined;
    const sig = try Signal.encodeInto(&sb, .{ .type = "go", .payload = "" });
    try testing.expect((try f.command(.workflow_signal, view_mod.idKey(&kb, .default, "gone") catch unreachable, sig, 5)).refused() != null);
    try testing.expectEqual(sent, (try f.chunks()).len);
    try testing.expect(f.get("gone") == null);
}

test "workflow runs machine: an ended run keeps a summary: a preview of its output, its whole output sent, no input, tags or step outputs" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    try testing.expect((try f.step("r1", .{ .rev = 0, .output_step = "s1" })).refused() == null);
    var ob: [3000]u8 = undefined;
    @memset(&ob, 'o');
    var vb: [4096]u8 = undefined;
    const value = (Complete{ .status = .completed, .output = &ob, .event = "workflow_completed", .detail = "", .workflow = "wf" }).encode(&vb).?;
    try testing.expect((try f.complete("r1", value, 9)).refused() == null);
    const run = f.get("r1").?;
    try testing.expectEqual(@as(usize, OUTPUT_PREVIEW), run.output.len);
    try testing.expect(run.output_truncated);
    try testing.expectEqualStrings("", run.input);
    try testing.expectEqualStrings("", run.search_tags);
    var none = (ReadView{ .store = &f.store }).range(OutFam, try runRange(f.scratch.allocator(), .default, "r1"));
    try testing.expect((try none.next()) == null);
    // The whole output was sent: over half a chunk, at once, then the
    // last chunk.
    const sent = try f.chunks();
    try testing.expect(sent[sent.len - 1].final);
    var whole = false;
    for (sent) |c| whole = whole or (std.mem.indexOf(u8, c.records, OUTPUT_EVENT) != null and std.mem.indexOf(u8, c.records, &ob) != null);
    try testing.expect(whole);
}

test "workflow runs machine: a step's call is recorded and its start sent with it; a step taking its end applies only once it settled" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var cb: [ae.Call.max_size]u8 = undefined;
    const call = try ae.Call.encodeInto(&cb, .{ .ns = NamespaceId.default.int(), .kind = .action, .caller_run = "r1", .caller_workflow = "wf", .step = "s", .target = "act", .version = "", .run = "a1", .input = "{}", .can_wait = false });
    const sent = f.store.families[channels.OutFam.ID].count;
    try testing.expect((try f.step("r1", .{ .rev = 0, .wait = .call, .wait_on = "a1", .call = call })).refused() == null);
    const slot = calls_mod.callerSlot(.default, "wf");
    try testing.expectEqual(calls_mod.State.sent, calls_mod.callOf(.{ .store = &f.store }, .default, slot, "a1").?.state);
    try testing.expectEqual(sent + 1, f.store.families[channels.OutFam.ID].count);
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.waiting)), f.get("r1").?.status);
    // A call that isn't the step's wait is a broken log.
    _ = try f.start("r2", "wf", "workflow_started", "", 1);
    try testing.expectError(error.Halt, f.step("r2", .{ .rev = 0, .wait = .call, .wait_on = "other", .call = call }));
    // Nor may a step leave the call unsettled.
    const left = try f.step("r1", .{ .rev = 1 });
    try testing.expectEqualStrings("conflict: the step leaves its run's call unsettled", left.refused().?.refusal.message);

    // Its end taken while the call is out: refused, the run waiting still.
    var r = try f.step("r1", .{ .rev = 1, .settles = "a1" });
    try testing.expectEqualStrings("conflict: the run's call hasn't ended", r.refused().?.refusal.message);
    // Another callee's end: refused.
    r = try f.step("r1", .{ .rev = 1, .settles = "a2" });
    try testing.expect(r.refused() != null);
    // Its run ends: then the step taking it applies.
    var db: [ae.Done.max_size]u8 = undefined;
    _ = try f.answer(.call_done, try ae.Done.encodeInto(&db, .{ .ns = NamespaceId.default.int(), .slot = slot, .run = "a1", .status = .completed, .outcome = "", .result = "{}" }));
    try testing.expect((try f.step("r1", .{ .rev = 1, .settles = "a1" })).refused() == null);
    try testing.expectEqual(Wait.none, f.get("r1").?.wait);
    // Its record went with it.
    try testing.expect(calls_mod.callOf(.{ .store = &f.store }, .default, slot, "a1") == null);
}

test "workflow runs machine: a call settled refused stays refused when its run's end arrives after" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var cb: [ae.Call.max_size]u8 = undefined;
    const call = try ae.Call.encodeInto(&cb, .{ .ns = NamespaceId.default.int(), .kind = .action, .caller_run = "r1", .caller_workflow = "wf", .step = "s", .target = "act", .version = "", .run = "a1", .input = "{}", .can_wait = false });
    _ = try f.step("r1", .{ .rev = 0, .wait = .call, .wait_on = "a1", .call = call });
    const slot = calls_mod.callerSlot(.default, "wf");
    var rb: [ae.Reply.max_size]u8 = undefined;
    _ = try f.answer(.call_reply, try ae.Reply.encodeInto(&rb, .{ .ns = NamespaceId.default.int(), .slot = slot, .run = "a1", .answer = .skipped, .why = "repaired" }));
    var db: [ae.Done.max_size]u8 = undefined;
    _ = try f.answer(.call_done, try ae.Done.encodeInto(&db, .{ .ns = NamespaceId.default.int(), .slot = slot, .run = "a1", .status = .completed, .outcome = "", .result = "{}" }));
    const c = calls_mod.callOf(.{ .store = &f.store }, .default, slot, "a1").?;
    try testing.expectEqual(calls_mod.State.refused, c.state);
    try testing.expectEqualStrings("repaired", c.outcome);
    try testing.expectEqualStrings("", c.result);
}

test "workflow runs machine: a repair skip leaves a call held over its callee's memory share as it is" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var cb: [ae.Call.max_size]u8 = undefined;
    const call = try ae.Call.encodeInto(&cb, .{ .ns = NamespaceId.default.int(), .kind = .action, .caller_run = "r1", .caller_workflow = "wf", .step = "s", .target = "act", .version = "", .run = "a1", .input = "{}", .can_wait = true });
    _ = try f.step("r1", .{ .rev = 0, .wait = .call, .wait_on = "a1", .call = call });
    const slot = calls_mod.callerSlot(.default, "wf");
    var rb: [ae.Reply.max_size]u8 = undefined;
    const held = try ae.Reply.encodeInto(&rb, .{ .ns = NamespaceId.default.int(), .slot = slot, .run = "a1", .answer = .over_budget, .why = "would pass its share" });
    _ = try f.answer(.call_reply, held);
    try testing.expectEqual(calls_mod.State.over_budget, calls_mod.callOf(.{ .store = &f.store }, .default, slot, "a1").?.state);
    // A skip of a later answer to it: the hold stands, to be retried.
    f.index += 1;
    const e = entry_mod.buildEntry(.call_reply, entry_mod.Flags.NONE, 1, f.index, 0, held);
    _ = f.scratch.reset(.retain_capacity);
    var ks = view_mod.KeySet.init(f.scratch.allocator());
    try machine.keys(&e, &ks);
    var view = view_mod.View.init(&f.store, f.scratch.allocator(), &ks);
    try machine.skipped.?(&view, &e, "repaired");
    try view.commit();
    try testing.expectEqual(calls_mod.State.over_budget, calls_mod.callOf(.{ .store = &f.store }, .default, slot, "a1").?.state);
}

test "workflow runs machine: a step whose counters are past their bound is refused, writing nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    const big: [MAX_PROGRESS + 1]u8 = @splat(1);
    const r = try f.step("r1", .{ .rev = 0, .progress = &big });
    try testing.expectEqualStrings("a step's counters are past their bound", r.refused().?.refusal.message);
    try testing.expectEqual(@as(u64, 0), f.get("r1").?.rev);
    try testing.expect((try f.step("r1", .{ .rev = 0, .progress = big[0..MAX_PROGRESS] })).refused() == null);
}

test "workflow runs machine: a disabled workflow refuses a client or schedule start, not a trigger's, and refuses a child to its parent without creating it" {
    var f = Fixture.init();
    defer f.deinit();
    try f.create("wf");
    try f.disable("wf");
    var r = try f.start("c", "wf", "workflow_started", "", 3);
    try testing.expectEqualStrings("workflow is disabled", r.refused().?.refusal.message);
    r = try f.start("s", "wf", "schedule_started", "_schedule:16:wf:4000", 3);
    try testing.expect(r.refused() != null);
    try testing.expect(f.active("wf") == null);
    r = try f.start("t", "wf", "trigger_started", "_trigger:ev@5:2", 3);
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

/// A trigger's start of run `run_id` from where `at` says it read.
fn triggerStart(f: *Fixture, run_id: []const u8, idem: []const u8, at: TriggerAt) !Result {
    var kb: [64]u8 = undefined;
    var vb: [512]u8 = undefined;
    const value = (Start{ .workflow = "wf", .version = "1", .status = .running, .event = "trigger_started", .idem = if (idem.len > 0) idem else null, .trigger = at, .input = "" }).encode(&vb).?;
    return f.command(.workflow_start, view_mod.idKey(&kb, .default, run_id) catch unreachable, value, 1);
}

fn placeOf(f: *Fixture) [5]u64 {
    const p = f.active("wf").?;
    return .{ p.trig_ns, p.trig_made, if (std.mem.eql(u8, p.trig_stream, "ev")) 1 else if (p.trig_stream.len == 0) 0 else 2, p.trig_ts, p.trig_seq };
}

test "workflow runs machine: a trigger's place goes on in its stream, never back, and a start read from an older making of it moves nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try triggerStart(&f, "a", "", .{ .ns = 16, .made = 5, .stream = "ev", .ts = 10, .seq = 2 });
    try testing.expectEqual([5]u64{ 16, 5, 1, 10, 2 }, placeOf(&f));
    // An earlier event, its start applied later: the place stays.
    _ = try triggerStart(&f, "b", "", .{ .ns = 16, .made = 5, .stream = "ev", .ts = 10, .seq = 1 });
    try testing.expectEqual([5]u64{ 16, 5, 1, 10, 2 }, placeOf(&f));
    _ = try triggerStart(&f, "c", "", .{ .ns = 16, .made = 5, .stream = "ev", .ts = 11, .seq = 0 });
    try testing.expectEqual([5]u64{ 16, 5, 1, 11, 0 }, placeOf(&f));
    // The stream made again: its events from its start.
    _ = try triggerStart(&f, "d", "", .{ .ns = 16, .made = 9, .stream = "ev", .ts = 1, .seq = 0 });
    try testing.expectEqual([5]u64{ 16, 9, 1, 1, 0 }, placeOf(&f));
    // A start read from the making before: stale, nothing moves.
    _ = try triggerStart(&f, "e", "", .{ .ns = 16, .made = 5, .stream = "ev", .ts = 50, .seq = 0 });
    try testing.expectEqual([5]u64{ 16, 9, 1, 1, 0 }, placeOf(&f));
    // Another stream, or one of its name in another namespace: a new place.
    _ = try triggerStart(&f, "g", "", .{ .ns = 16, .made = 3, .stream = "other", .ts = 7, .seq = 0 });
    try testing.expectEqual([5]u64{ 16, 3, 2, 7, 0 }, placeOf(&f));
    _ = try triggerStart(&f, "h", "", .{ .ns = 17, .made = 1, .stream = "ev", .ts = 2, .seq = 0 });
    try testing.expectEqual([5]u64{ 17, 1, 1, 2, 0 }, placeOf(&f));
    // A start refused (its run exists) moves nothing.
    const r = try triggerStart(&f, "h", "", .{ .ns = 17, .made = 1, .stream = "ev", .ts = 99, .seq = 0 });
    try testing.expect(r.refused() != null);
    try testing.expectEqual([5]u64{ 17, 1, 1, 2, 0 }, placeOf(&f));
}

test "workflow runs machine: a trigger's start read again, its run already started, still moves the place" {
    var f = Fixture.init();
    defer f.deinit();
    const key = "_trigger:16:5:ev@10:2";
    _ = try triggerStart(&f, "a", key, .{ .ns = 16, .made = 5, .stream = "ev", .ts = 3, .seq = 0 });
    const again = try triggerStart(&f, "b", key, .{ .ns = 16, .made = 5, .stream = "ev", .ts = 10, .seq = 2 });
    try testing.expect(again.refused() == null);
    try testing.expectEqual(@as(?Run.Value, null), f.get("b"));
    try testing.expectEqual([5]u64{ 16, 5, 1, 10, 2 }, placeOf(&f));
    try testing.expectEqual(@as(u32, 1), f.active("wf").?.active);
}

test "workflow runs machine: a key with a leading '_' is the server's: refused on any start but a schedule's or a trigger's, by its event, and moves nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try triggerStart(&f, "t", "_trigger:16:5:ev@10:2", .{ .ns = 16, .made = 5, .stream = "ev", .ts = 10, .seq = 2 });
    _ = try f.start("s", "wf", "schedule_started", "_schedule:16:wf:4000", 1);
    const before = f.active("wf").?;
    for ([_][2][]const u8{
        .{ "workflow_started", "_trigger:16:5:ev@99:0" },
        .{ "workflow_started", "_trigger:16:5:ev@10:2" },
        .{ "workflow_started", "_schedule:16:wf:9000" },
        .{ "workflow_started", "_schedule:16:wf:4000" },
        .{ "workflow_started", "_anything" },
        .{ "child_started", "_schedule:16:wf:9000" },
    }) |c| {
        const r = try f.start("x", "wf", c[0], c[1], 2);
        try testing.expectEqualStrings(SERVER_KEY_REFUSED, r.refused().?.refusal.message);
        try testing.expectEqual(@as(?Run.Value, null), f.get("x"));
    }
    const after = f.active("wf").?;
    try testing.expectEqual(before.fired_ms, after.fired_ms);
    try testing.expectEqual(before.trig_ts, after.trig_ts);
    try testing.expectEqual(before.active, after.active);
    // The gate is the start's event: a schedule's start with a key of any
    // shape applies.
    try testing.expect((try f.start("y", "wf", "schedule_started", "_anything", 2)).refused() == null);
}

test "workflow runs machine: keys without a leading '_' are a client's or a child's as ever" {
    var f = Fixture.init();
    defer f.deinit();
    for ([_][3][]const u8{
        .{ "c", "workflow_started", "order-17" },
        .{ "d", "workflow_started", "trigger:16:5:ev@10:2" },
        .{ "e", "workflow_started", "schedule:16:wf:4000" },
        .{ "k", "child_started", "kid-key" },
    }) |c| {
        try testing.expect((try f.start(c[0], "wf", c[1], c[2], 2)).refused() == null);
        try testing.expect(f.get(c[0]) != null);
    }
    // Neither moves a schedule's slot or a trigger's place.
    try testing.expectEqual(@as(i64, 0), f.active("wf").?.fired_ms);
    try testing.expectEqual(@as(u64, 0), f.active("wf").?.trig_made);
}

test "workflow runs machine: a trigger's start that doesn't say where it read, or another start that does, halts" {
    var f = Fixture.init();
    defer f.deinit();
    var kb: [64]u8 = undefined;
    var vb: [256]u8 = undefined;
    const bare = (Start{ .workflow = "wf", .version = "1", .status = .running, .event = "trigger_started", .idem = null, .input = "" }).encode(&vb).?;
    try testing.expectError(error.Halt, f.command(.workflow_start, view_mod.idKey(&kb, .default, "a") catch unreachable, bare, 1));
    const said = (Start{ .workflow = "wf", .version = "1", .status = .running, .event = "workflow_started", .idem = null, .trigger = .{ .ns = 16, .made = 1, .stream = "ev", .ts = 1, .seq = 0 }, .input = "" }).encode(&vb).?;
    try testing.expectError(error.Halt, f.command(.workflow_start, view_mod.idKey(&kb, .default, "b") catch unreachable, said, 1));
}

test "workflow runs machine: a schedule's fired slot never moves back" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("a", "wf", "schedule_started", "_schedule:16:wf:4000", 1);
    _ = try f.start("b", "wf", "schedule_started", "_schedule:16:wf:2000", 1);
    try testing.expectEqual(@as(i64, 4000), f.active("wf").?.fired_ms);
    // A client's start with a key of that shape fires nothing.
    _ = try f.start("c", "wf", "workflow_started", "_schedule:16:wf:9000", 1);
    try testing.expectEqual(@as(i64, 4000), f.active("wf").?.fired_ms);
}

test "workflow runs machine: a signal is kept and said; one a run waits for wakes it and moves its rev, so a step decided from the wait is refused" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    // One it doesn't wait for: kept, and nothing else moves.
    var r = try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "early", .payload = "p" }), 5);
    try testing.expect(r.refused() == null);
    try testing.expectEqual(@as(u64, 0), f.get("r1").?.rev);
    try testing.expect(try hasSignal(testing.allocator, .{ .store = &f.store }, key, "early"));
    try testing.expect(!try hasSignal(testing.allocator, .{ .store = &f.store }, key, "late"));

    // It waits on "go", with a deadline: a signal of it wakes the run.
    try testing.expect((try f.step("r1", .{ .rev = 0, .wait = .signal, .wait_on = "go" })).refused() == null);
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.waiting)), f.get("r1").?.status);
    r = try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "go", .payload = "" }), 6);
    try testing.expect(r.refused() == null);
    const woken = f.get("r1").?;
    try testing.expectEqual(Wait.none, woken.wait);
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.running)), woken.status);
    try testing.expectEqual(@as(u64, 2), woken.rev);
    try testing.expectEqual(@as(u64, 2), woken.sig_next);
    // Its timeout, decided from the wait at rev 1: refused, not applied too.
    const stale = try f.step("r1", .{ .rev = 1, .ends = .timed_out });
    try testing.expectEqualStrings("conflict: the run moved on from that step", stale.refused().?.refusal.message);
    try testing.expectEqual(@as(u8, @backingInt(RunStatus.running)), f.get("r1").?.status);
}

test "workflow runs machine: a wait decided before its signal applied, on a signal the run then holds, is refused" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    // Its leader decided "wait on go" at rev 0; the signal applies first.
    try testing.expect((try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "go", .payload = "" }), 2)).refused() == null);
    const stale = try f.step("r1", .{ .rev = 0, .wait = .signal, .wait_on = "go" });
    try testing.expectEqualStrings("conflict: the run holds that signal already", stale.refused().?.refusal.message);
    const run = f.get("r1").?;
    try testing.expectEqual(Wait.none, run.wait);
    try testing.expectEqual(@as(u64, 0), run.rev);
    // A wait on another type still applies.
    try testing.expect((try f.step("r1", .{ .rev = 0, .wait = .signal, .wait_on = "other" })).refused() == null);
}

test "workflow runs machine: a wait takes the oldest signal of its type, so two waits need two, and a step taking one the run doesn't hold is refused" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    try testing.expect((try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "approve", .payload = "one" }), 2)).refused() == null);
    try testing.expect((try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "approve", .payload = "two" }), 3)).refused() == null);
    // Its first wait takes the older; the second is still held.
    try testing.expect((try f.step("r1", .{ .rev = 0, .takes = &.{"approve"} })).refused() == null);
    try testing.expectEqual(@as(u64, 1), f.get("r1").?.sig_held);
    var it = (ReadView{ .store = &f.store }).range(SigFam, "");
    try testing.expectEqualStrings("two", (try it.next()).?.value.v.payload);
    try testing.expect((try it.next()) == null);
    // Two more waits, one signal: refused, nothing taken.
    const short = try f.step("r1", .{ .rev = 1, .takes = &.{ "approve", "approve" } });
    try testing.expectEqualStrings("conflict: the run holds no such signal", short.refused().?.refusal.message);
    try testing.expectEqual(@as(u64, 1), f.get("r1").?.sig_held);
    // The second wait takes the other, and a third waits.
    try testing.expect((try f.step("r1", .{ .rev = 1, .takes = &.{"approve"}, .wait = .signal, .wait_on = "approve" })).refused() == null);
    try testing.expectEqual(@as(u64, 0), f.get("r1").?.sig_held);
    try testing.expectEqual(Wait.signal, f.get("r1").?.wait);
}

test "workflow runs machine: a step that takes a signal, refused because the run moved on first or ended, takes nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    try testing.expect((try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "go", .payload = "" }), 2)).refused() == null);
    // Another step from rev 0 (its timeout) applies first.
    try testing.expect((try f.step("r1", .{ .rev = 0, .wait = .timer })).refused() == null);
    const stale = try f.step("r1", .{ .rev = 0, .takes = &.{"go"} });
    try testing.expectEqualStrings("conflict: the run moved on from that step", stale.refused().?.refusal.message);
    try testing.expectEqual(@as(u64, 1), f.get("r1").?.sig_held);
    try testing.expect(try hasSignal(testing.allocator, .{ .store = &f.store }, key, "go"));
    // A cancel ends it holding two no wait took: they go with it, and a
    // step taking one is refused.
    try testing.expect((try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = "go", .payload = "" }), 3)).refused() == null);
    try testing.expectEqual(@as(u64, 2), f.get("r1").?.sig_held);
    var vb: [512]u8 = undefined;
    _ = try f.complete("r1", completeValue(&vb, .cancelled, "wf"), 4);
    const ended = try f.step("r1", .{ .rev = 1, .takes = &.{"go"} });
    try testing.expectEqualStrings("conflict: the run has ended", ended.refused().?.refusal.message);
    try testing.expectEqual(@as(u64, 0), f.get("r1").?.sig_held);
    var it = (ReadView{ .store = &f.store }).range(SigFam, "");
    try testing.expect((try it.next()) == null);
}

test "workflow runs machine: a run its own step ends drops the signals it holds" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    for ([_][]const u8{ "a", "b" }) |t| try testing.expect((try f.command(.workflow_signal, key, try Signal.encodeInto(&sb, .{ .type = t, .payload = "x" }), 2)).refused() == null);
    try testing.expect((try f.step("r1", .{ .rev = 0, .ends = .completed })).refused() == null);
    try testing.expectEqual(@as(u64, 0), f.get("r1").?.sig_held);
    var it = (ReadView{ .store = &f.store }).range(SigFam, "");
    try testing.expect((try it.next()) == null);
}

test "workflow runs machine: a run that takes each signal it is sent never fills up" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    const value = try Signal.encodeInto(&sb, .{ .type = "go", .payload = "" });
    for (0..100) |i| {
        try testing.expect((try f.command(.workflow_signal, key, value, 2)).refused() == null);
        try testing.expect((try f.step("r1", .{ .rev = i, .takes = &.{"go"} })).refused() == null);
    }
    const run = f.get("r1").?;
    try testing.expectEqual(@as(u64, 100), run.sig_next);
    try testing.expectEqual(@as(u64, 0), run.sig_held);
}

test "workflow runs machine: a run takes 64 signals and refuses the next; an ended or missing run refuses one" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.start("r1", "wf", "workflow_started", "", 1);
    var kb: [64]u8 = undefined;
    const key = view_mod.idKey(&kb, .default, "r1") catch unreachable;
    var sb: [128]u8 = undefined;
    const value = try Signal.encodeInto(&sb, .{ .type = "same", .payload = "" });
    // Every one counts, however many share a type.
    for (0..MAX_SIGNALS) |_| try testing.expect((try f.command(.workflow_signal, key, value, 2)).refused() == null);
    const full = try f.command(.workflow_signal, key, value, 2);
    try testing.expectEqualStrings("run's signals are full", full.refused().?.refusal.message);
    try testing.expectEqual(MAX_SIGNALS, f.get("r1").?.sig_next);

    var vb: [512]u8 = undefined;
    _ = try f.complete("r1", completeValue(&vb, .cancelled, "wf"), 3);
    const ended = try f.command(.workflow_signal, key, value, 4);
    try testing.expectEqualStrings("run already finished", ended.refused().?.refusal.message);
    const missing = try f.command(.workflow_signal, view_mod.idKey(&kb, .default, "nope") catch unreachable, value, 4);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.not_found, missing.refused().?.status);
}
