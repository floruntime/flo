//! Workflow runs as the server keeps them: the runs machine
//! (`workflow/runs_machine.zig`) on the definitions' store (a start reads
//! whether its workflow is enabled), the checker comparing its families'
//! checksums, and each entry checked against a ledger of the runs the
//! entries made, kept apart from the machine:
//!
//! - a start creates its run, its first history event, its idempotency key
//!   and its workflow's count; one whose key started a run already creates
//!   nothing, one whose run exists is refused, a client or schedule start
//!   of a disabled workflow is refused, and a disabled child creates no run
//!   and is refused to its parent without the entry being refused;
//! - a schedule start fires its slot, which never moves back; no other
//!   start moves it, whatever its key;
//! - a step applies only at its run's rev, and never after the run ends;
//!   it sets what the run waits for, and may end it;
//! - a step leaves its run's call only by taking its end (once the call
//!   has ended or is gone, its record going with it) or by ending the run;
//!   a step's call is recorded, and its start sent, once per (workflow's
//!   slot, callee);
//! - a call settles once: a reply answers only a sent call; its run's end
//!   moves it to done only from sent or started, never over a refusal or
//!   no action; a retry asks again only for one no action took, sending
//!   its start again; a clear drops it;
//! - a completion ends its run once, with its history (the last 256) and
//!   step outputs; another with the same status changes nothing, one with
//!   another status is refused;
//! - a child's parent hears its start (only a child start's), its refusal,
//!   a disabled workflow, and its end, each once;
//! - a signal is held by its run, refused when the run is gone, has ended
//!   or holds 64 no wait took; one of the type the run waits on wakes it,
//!   moving its rev, and is still held. A step names the types its waits
//!   took, each the earliest held of its type not already named; a step
//!   naming one the run doesn't hold, or waiting on a type the run still
//!   holds after its takes, is refused, and only a step that applies takes
//!   anything. A run's end drops every signal it holds;
//! - the store holds what the ledger holds, and on its own: each workflow's
//!   count is its runs not ended, each run's history is its last seqs, each
//!   idempotency key names a run of that workflow with that key, a run
//!   waits exactly when its status says so, and each recorded call's caller
//!   has ended or waits on it.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const view_mod = @import("../contract/view.zig");
const channels = @import("../contract/channels.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const router = @import("../node/router.zig");
const rm = @import("../workflow/runs_machine.zig");
const calls_mod = @import("../actions/calls_machine.zig");
const hm = @import("../stream/history_machine.zig");
const history_lane = @import("history_lane.zig");
const ce = @import("../protocol/channel_entries.zig");
const defs_family = @import("workflow_defs_family.zig");

const Allocator = std.mem.Allocator;
const EntryType = entry_mod.EntryType;
const RunStatus = rm.RunStatus;
const NS: @import("../namespace/table.zig").Id = .default;
const NAMES = defs_family.NAMES;

/// Runs are never removed, so the workload has room for each to start.
const RUNS = blk: {
    @setEvalBranchQuota(100_000);
    var out: [128][]const u8 = undefined;
    for (&out, 0..) |*x, i| x.* = std.fmt.comptimePrint("r{d}", .{i});
    break :blk out;
};
/// Client keys, and a schedule's (its slot last): slots out of order, so a
/// later start can carry an earlier slot.
const IDEMS = [_][]const u8{ "k0", "k1", "_schedule:16:w:3000", "_schedule:16:w:1000", "_schedule:16:w:2000", "_trigger:16:1:ev@1:0", "_trigger:16:2:ev@3:1" };
const SLOT_OF = [_]?i64{ null, null, 3000, 1000, 2000, null, null };
/// Where a trigger's start says it read: a few namespaces, makings and
/// streams, so its place moves on, is replaced, and holds against a start
/// read from an older making.
const T_NS = [_]u32{ 16, 17 };
const T_MADE = [_]u64{ 1, 2 };
const T_STREAMS = [_][]const u8{ "ev", "other" };
const EVENTS = [_][]const u8{ "workflow_started", "schedule_started", "trigger_started", "child_started" };
const SCHEDULE = 1;
const TRIGGER = 2;
const CHILD = 3;
const STEPS = [_][]const u8{ "a", "b", "c" };
const OUTCOMES = [_][]const u8{ "done", "failed" };
const OUTPUTS = [_][]const u8{ "o0", "o1" };
const PARENT_RUNS = [_][]const u8{ "p0", "p1" };
const PARENT_SLOTS = [_]u16{ 3, 700 };
const ENDS = [_]RunStatus{ .completed, .failed, .cancelled, .timed_out };
const INPUT = "{}";
/// The whole store is checked at every index this divides, as the
/// checksums are sampled; the run an entry names, at every one.
const SWEEP_EVERY = 16;
const DISABLED_WHY = "workflow is disabled";

/// A step's callees: few, so two runs of a workflow (or of two workflows
/// on one slot) call one now and then, and share its record.
const CALLEES = [_][]const u8{ "c0", "c1", "c2", "c3", "c4", "c5" };
const ACTION_TARGET = "act";
const CHILD_TARGET = "cw";
const SIGNAL = "sig";
/// Signal types: a run waits on one of these, and is sent them.
const SIG_TYPES = [_][]const u8{ SIGNAL, "go", "stop" };
const SIG_PAYLOADS = [_][]const u8{ "", "p1" };
/// A run holds at most this many signals no wait took.
const SIGNAL_BOUND = 64;
comptime {
    if (rm.MAX_SIGNALS != SIGNAL_BOUND) @compileError("the runs machine's signal bound is not the specification's 64");
}
const WAIT_ONS = CALLEES ++ SIG_TYPES;
const PROGRESS = [_][]const u8{ "", "{\"n\":1}" };
/// Past what a step's counters may hold: refused.
const OVERSIZED: [rm.MAX_PROGRESS + 1]u8 = @splat('x');
const WHYS = [_][]const u8{"lost"};
const DONE_OUTCOMES = [_][]const u8{ "", "ok" };
const RESULTS = [_][]const u8{ "", "r0" };

/// A run's history past its start's event: a step's events are "e" at 0
/// ms, a cancel's is "c" at its entry's stamp. Not named by seq: a step
/// made at a rev ahead of the leader's may apply after another step.
const STEP_EVENT = "e";
const CANCEL_EVENT = "c";

/// The one of `list` equal to `s`, so the ledger holds no entry's bytes.
fn static(comptime list: []const []const u8, s: []const u8) ?[]const u8 {
    for (list) |x| if (std.mem.eql(u8, x, s)) return x;
    return null;
}

fn targetOf(kind: ae.CallKind) []const u8 {
    return switch (kind) {
        .action => ACTION_TARGET,
        .workflow => CHILD_TARGET,
    };
}

/// A name's slot: a call's target's, and a workflow's, which its runs'
/// calls are keyed under and answered on.
fn slotOf(name: []const u8) u16 {
    return @intCast(router.nsKeySlot(NS, name));
}

/// The first workflow whose runs share `w`'s slot, which its calls are
/// keyed under.
fn slotOwner(w: usize) usize {
    return slotOwnerOf(slotOf(NAMES[w])).?;
}

fn slotOwnerOf(slot: u16) ?usize {
    for (NAMES, 0..) |name, i| if (slotOf(name) == slot) return i;
    return null;
}

pub const WorkflowRuns = struct {
    scratch: std.heap.ArenaAllocator,
    /// What landed of each run's history.
    landed: history_lane.Landed = .{},
    ledger: Ledger = .{},

    /// What a run waits for, as its last step set it.
    const WaitL = struct {
        wait: rm.Wait = .none,
        kind: ae.CallKind = .action,
        slot: u16 = 0,
        on: []const u8 = "",
        due_ms: i64 = 0,
        timeout_to: []const u8 = "",
    };

    const RunL = struct {
        wf: usize,
        status: RunStatus,
        stepped: bool,
        idem: ?usize,
        created_ms: i64,
        completed_ms: i64 = 0,
        output: ?usize = null,
        hist_next: u64,
        /// The start's event, at seq 0.
        started: usize,
        /// Its steps taken: a step applies only at this.
        rev: u64 = 0,
        /// The seq of a cancel's event, at `completed_ms`.
        ended_at: ?u64 = null,
        outs: [STEPS.len]?usize = @splat(null),
        step: ?usize = null,
        wait: WaitL = .{},
        progress: []const u8 = "",
        /// Signals it holds that no wait took, earliest first, and the
        /// seq the next one is given.
        held: [SIGNAL_BOUND]SigL = undefined,
        held_n: usize = 0,
        sig_next: u64 = 0,
        /// Its rev after a signal woke it, while no step has moved it since.
        woken_rev: ?u64 = null,
        /// What each of its last `HIST_RING` events is, by seq: a step's,
        /// or a signal's. More than a run's pending chunk ever holds.
        hist: [HIST_RING]HistL = undefined,
    };
    const SigL = struct { type: usize, payload: usize, seq: u64, at_ms: i64 };
    const HIST_RING = 256;
    comptime {
        // The smallest event a run's pending chunk holds is a step's.
        std.debug.assert(rm.HIST_CHUNK / (8 + 4 + STEP_EVENT.len + 4 + 8) < HIST_RING);
    }
    const HistL = struct { kind: enum(u8) { step, received, matched }, type: u8 = 0, at_ms: i64 = 0 };

    /// What happened, by class: each is reached by some seed of a swarm,
    /// or the swarm fails.
    pub const Class = enum {
        /// A step decided before a signal woke its run, refused for the rev.
        wake_moved_rev,
        /// A step taking a signal its run doesn't hold.
        takes_missing,
        /// A step waiting on a signal its run holds.
        stale_wait,
        /// A signal to a run holding as many as it may.
        signal_full,
        signal_to_ended,
        signal_to_unknown,
        /// A run ending while it holds signals.
        ended_holding,
        /// A step taking two of one type, applied.
        takes_repeat,
        /// A call's start answered over its action's shard's memory share.
        call_held,
        /// A held call asked for again.
        held_call_retried,
    };
    const PerDefL = struct { active: u32, fired_ms: i64, place: ?PlaceL = null };
    /// A trigger's place: the stream (namespace, making, name) and how far in.
    const PlaceL = struct { ns: u32, made: u64, stream: usize, ts: u64, seq: u64 };

    /// Where a trigger's start read moves its workflow's place. Another
    /// stream, or a later making of this one, is a new place; an earlier
    /// making is a stale read and changes nothing; in this making the place
    /// only goes forward.
    fn placeAfter(was: ?PlaceL, at: PlaceL) PlaceL {
        const p = was orelse return at;
        if (p.ns != at.ns or p.stream != at.stream) return at;
        if (at.made > p.made) return at;
        if (at.made < p.made) return p;
        if (at.ts < p.ts or (at.ts == p.ts and at.seq <= p.seq)) return p;
        return at;
    }
    const ParentL = struct { slot: usize, parent_run: usize, started: bool };
    /// A step's call, under its workflow's slot and its callee.
    const CallL = struct {
        kind: ae.CallKind,
        caller: usize,
        wf: usize,
        step: usize,
        can_wait: bool,
        state: calls_mod.State,
        attempts: u32,
        first_ns: u64,
        last_ns: u64,
        status: u8 = 0,
        outcome: []const u8 = "",
        result: []const u8 = "",
    };

    const Ledger = struct {
        runs: [RUNS.len]?RunL = @splat(null),
        per_def: [NAMES.len]?PerDefL = @splat(null),
        idem: [NAMES.len][IDEMS.len]?usize = @splat(@splat(null)),
        parents: [RUNS.len]?ParentL = @splat(null),
        /// By `slotOwner` of the caller's workflow, then callee.
        calls: [NAMES.len][CALLEES.len]?CallL = @splat(@splat(null)),
    };

    pub const FAMILIES = [_]u16{ rm.RunFam.ID, hm.Workflows.ID, rm.OutFam.ID, rm.IdemFam.ID, rm.PerDefFam.ID, rm.Parents.ID, calls_mod.Calls.ID, rm.SigFam.ID };
    pub const FAMILY_NAMES = [_][]const u8{ "workflow.run", "history.wf", "workflow.out", "workflow.idem", "workflow.per_def", "workflow.parents", "step.calls", "workflow.sig" };

    pub const Kind = enum { start, step, complete, child_start, child_refuse, reply, done, retry, clear, signal };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        broke: ?[]const u8 = null,
        classes: Classes = .empty,
    };

    pub const Classes = std.EnumSet(Class);

    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),
        classes: [std.enums.values(Class).len]u64 = @splat(0),

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
            var it = o.classes.iterator();
            while (it.next()) |c| self.classes[@backingInt(c)] += 1;
        }

        pub fn add(self: *Counts, o: Counts) void {
            for (&self.applied, o.applied) |*a, b| a.* += b;
            for (&self.refused, o.refused) |*a, b| a.* += b;
            for (&self.classes, o.classes) |*a, b| a.* += b;
        }
    };

    pub fn init(allocator: Allocator) WorkflowRuns {
        return .{ .scratch = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *WorkflowRuns) void {
        self.landed.deinit(self.scratch.child_allocator);
        self.scratch.deinit();
    }

    /// Replay re-applies from the start: the ledger goes with the store.
    pub fn reset(self: *WorkflowRuns) void {
        self.ledger = .{};
        self.landed.clear(self.scratch.child_allocator);
    }

    pub fn owns(entry_type: u8) bool {
        return switch (@as(EntryType, @fromBackingInt(entry_type))) {
            .workflow_start, .workflow_step, .workflow_complete, .child_start, .child_refuse, .workflow_signal => true,
            .call_reply, .call_done, .step_call_retry, .step_call_clear => true,
            else => false,
        };
    }

    fn indexOf(comptime list: []const []const u8, s: []const u8) ?usize {
        for (list, 0..) |x, i| if (std.mem.eql(u8, x, s)) return i;
        return null;
    }

    fn slotIndex(slot: u16) ?usize {
        for (PARENT_SLOTS, 0..) |s, i| if (s == slot) return i;
        return null;
    }

    /// A message the ledger says the entry sends: a parent's answer, or a
    /// call's start.
    const Send = struct {
        entry_type: EntryType,
        to: u16,
        run: []const u8,
        answer: ?ae.Answer = null,
        why: ?[]const u8 = null,
        done: ?ae.DoneStatus = null,
        result: []const u8 = "",
        /// A start's caller run, and a child start's parent slot.
        caller: []const u8 = "",
        parent_slot: u16 = 0,
    };
    /// What an entry does: its sends, or refused, with why the ledger
    /// refuses it, which a failure names.
    const Want = struct { refused: ?[]const u8 = null, sends: [2]?Send = @splat(null), classes: Classes = .empty };

    fn refuse(comptime kind: Kind, comptime why: []const u8) Want {
        return .{ .refused = "the runs machine applied a " ++ @tagName(kind) ++ " the ledger refuses: " ++ why };
    }

    fn refuseAs(comptime kind: Kind, comptime why: []const u8, class: Class) Want {
        var w = refuse(kind, why);
        w.classes.insert(class);
        return w;
    }

    /// Why a machine's refusal of an entry the ledger applies fails.
    const REFUSED_APPLIES = blk: {
        var out: [std.enums.values(Kind).len][]const u8 = undefined;
        for (std.enums.values(Kind), &out) |k, *x| x.* = "the runs machine refused a " ++ @tagName(k) ++ " the ledger applies";
        break :blk out;
    };

    fn doneOf(s: RunStatus) ae.DoneStatus {
        return switch (s) {
            .completed => .completed,
            .cancelled => .cancelled,
            .timed_out => .timed_out,
            else => .failed,
        };
    }

    /// The start a call sends to its target's slot.
    fn startOf(c: CallL, callee: usize) Send {
        return .{
            .entry_type = if (c.kind == .action) .action_invoke else .child_start,
            .to = slotOf(targetOf(c.kind)),
            .run = CALLEES[callee],
            .caller = RUNS[c.caller],
            .parent_slot = slotOf(NAMES[c.wf]),
        };
    }

    /// `run` ended `status`: no longer counted, its wait gone, and a
    /// child's parent told, in `want.sends[at]`.
    fn endL(l: *Ledger, r: usize, run: *RunL, status: RunStatus, output: ?usize, stamp_ms: i64, want: *Want, at: usize) void {
        run.status = status;
        run.completed_ms = stamp_ms;
        run.output = output;
        run.wait = .{};
        run.progress = "";
        // Its step outputs go: its history holds them.
        run.outs = @splat(null);
        // Its signals go with it.
        if (run.held_n > 0) want.classes.insert(.ended_holding);
        run.held_n = 0;
        run.woken_rev = null;
        if (l.per_def[run.wf]) |*per| per.active -|= 1;
        if (l.parents[r]) |f| {
            want.sends[at] = .{ .entry_type = .call_done, .to = PARENT_SLOTS[f.slot], .run = RUNS[r], .done = doneOf(status), .result = if (output) |o| OUTPUTS[o] else "" };
            l.parents[r] = null;
        }
    }

    /// What the ledger says `e` does, made so in the ledger: a refusal
    /// returns before it writes. Null for an entry the workload never makes.
    fn predict(self: *WorkflowRuns, defs: *const defs_family.WorkflowDefs, e: *const entry_mod.Entry, kind: Kind) ?Want {
        var want: Want = .{};
        const l = &self.ledger;
        const stamp_ms: i64 = @intCast(e.header.timestamp_ns / std.time.ns_per_ms);
        var diag: schema.Diagnostic = .{};
        switch (kind) {
            .child_start => {
                const p = ae.ChildStart.decodeAll(e.payload, &diag) catch return null;
                const r = indexOf(&RUNS, p.run) orelse return null;
                const slot = slotIndex(p.parent_slot) orelse return null;
                const pr = indexOf(&PARENT_RUNS, p.parent_run) orelse return null;
                if (l.parents[r]) |f| {
                    // The same call sent again: the first stands.
                    if (f.slot == slot and f.parent_run == pr) return want;
                    want.sends[0] = .{ .entry_type = .call_reply, .to = PARENT_SLOTS[slot], .run = RUNS[r], .answer = .conflict };
                    return want;
                }
                l.parents[r] = .{ .slot = slot, .parent_run = pr, .started = false };
            },
            .child_refuse => {
                const p = ae.ChildRefuse.decodeAll(e.payload, &diag) catch return null;
                const r = indexOf(&RUNS, p.run) orelse return null;
                const f = l.parents[r] orelse return want;
                if (f.started) return want;
                want.sends[0] = .{ .entry_type = .call_reply, .to = PARENT_SLOTS[f.slot], .run = RUNS[r], .answer = p.answer };
                l.parents[r] = null;
            },
            .reply, .done, .retry, .clear => return self.predictCall(e, kind),
            .start, .step, .complete, .signal => {
                const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
                const r = indexOf(&RUNS, cmd.key[4..]) orelse return null;
                switch (kind) {
                    .step => {
                        const st = rm.Step.decodeAll(cmd.value, &diag) catch return null;
                        var run = l.runs[r] orelse return refuse(.step, "the run isn't there");
                        if (!std.mem.eql(u8, st.workflow, NAMES[run.wf])) return null;
                        const ends: ?RunStatus = if (st.ends == 0) null else blk: {
                            const s = std.enums.fromInt(RunStatus, st.ends) orelse return null;
                            if (!s.isTerminal()) return null;
                            break :blk s;
                        };
                        if (run.status.isTerminal()) return refuse(.step, "the run has ended");
                        if (st.rev != run.rev) {
                            // Decided before a signal woke the run.
                            if (run.woken_rev == run.rev and st.rev +% 1 == run.rev) return refuseAs(.step, "the run moved on from that rev", .wake_moved_rev);
                            return refuse(.step, "the run moved on from that rev");
                        }
                        // The signals its waits took: each the earliest held
                        // of its type that this step hasn't named already.
                        var picked: [SIGNAL_BOUND]usize = undefined;
                        var np: usize = 0;
                        var repeats = false;
                        var takes = st.takes.iterator();
                        while (takes.next() catch return null) |t| {
                            const ty = indexOf(&SIG_TYPES, t.type) orelse return null;
                            for (picked[0..np]) |i| {
                                if (run.held[i].type == ty) repeats = true;
                            }
                            const i = earliestHeld(&run, ty, picked[0..np]) orelse return refuseAs(.step, "its takes name a signal its run doesn't hold", .takes_missing);
                            if (np == picked.len) return null;
                            picked[np] = i;
                            np += 1;
                        }
                        // Waiting on a type it would still hold: stale.
                        if (st.wait == .signal) if (indexOf(&SIG_TYPES, st.wait_on)) |ty| if (earliestHeld(&run, ty, picked[0..np]) != null)
                            return refuseAs(.step, "it waits on a signal its run holds", .stale_wait);
                        if (st.progress.len > rm.MAX_PROGRESS) return refuse(.step, "its counters are past their bound");
                        const leaves = run.wait.wait == .call and !(st.wait == .call and std.mem.eql(u8, st.wait_on, run.wait.on));
                        if (leaves and ends == null and st.settles.len == 0) return refuse(.step, "it leaves its run's call without taking its end");
                        const so = slotOwner(run.wf);
                        var settled: ?usize = null;
                        if (st.settles.len > 0) {
                            if (run.wait.wait != .call or !std.mem.eql(u8, run.wait.on, st.settles)) return refuse(.step, "it takes the end of a call its run doesn't wait on");
                            if (st.caller_slot != slotOf(NAMES[run.wf])) return null;
                            const c = indexOf(&CALLEES, st.settles) orelse return null;
                            if (l.calls[so][c]) |call| switch (call.state) {
                                .sent, .started => return refuse(.step, "it takes its run's call's end before the call has one"),
                                .done, .refused, .no_action, .over_budget => {},
                            };
                            settled = c;
                        }
                        // It applies: the signals it took go, the settled
                        // call's record goes, then its own is recorded.
                        if (np > 0) {
                            var kept: usize = 0;
                            for (run.held[0..run.held_n], 0..) |h, i| {
                                if (std.mem.indexOfScalar(usize, picked[0..np], i) != null) continue;
                                run.held[kept] = h;
                                kept += 1;
                            }
                            run.held_n = kept;
                            if (repeats) want.classes.insert(.takes_repeat);
                        }
                        if (settled) |c| l.calls[so][c] = null;
                        var sends: usize = 0;
                        if (st.call.len > 0) {
                            const p = ae.Call.decodeAll(st.call, &diag) catch return null;
                            const c = indexOf(&CALLEES, p.run) orelse return null;
                            if (st.wait != .call or !std.mem.eql(u8, p.run, st.wait_on) or !std.mem.eql(u8, p.caller_run, RUNS[r]) or
                                !std.mem.eql(u8, p.caller_workflow, NAMES[run.wf]) or !std.mem.eql(u8, p.target, targetOf(p.kind))) return null;
                            if (l.calls[so][c] == null) {
                                const at: u64 = @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms;
                                const call: CallL = .{
                                    .kind = p.kind,
                                    .caller = r,
                                    .wf = run.wf,
                                    .step = indexOf(&STEPS, p.step) orelse return null,
                                    .can_wait = p.can_wait,
                                    .state = .sent,
                                    .attempts = 1,
                                    .first_ns = at,
                                    .last_ns = at,
                                };
                                l.calls[so][c] = call;
                                want.sends[sends] = startOf(call, c);
                                sends += 1;
                            }
                        }
                        for (0..st.events.count) |_| {
                            run.hist[run.hist_next % HIST_RING] = .{ .kind = .step };
                            run.hist_next += 1;
                        }
                        var outs = st.outputs.iterator();
                        while (outs.next() catch return null) |o| {
                            run.outs[indexOf(&STEPS, o.step) orelse return null] = indexOf(&OUTCOMES, o.outcome) orelse return null;
                        }
                        run.rev += 1;
                        run.woken_rev = null;
                        run.stepped = true;
                        run.step = if (st.step.len == 0) null else indexOf(&STEPS, st.step) orelse return null;
                        run.wait = .{
                            .wait = st.wait,
                            .kind = st.wait_kind,
                            .slot = st.wait_slot,
                            .on = if (st.wait_on.len == 0) "" else static(&WAIT_ONS, st.wait_on) orelse return null,
                            .due_ms = st.wait_due_ms,
                            .timeout_to = if (st.wait_timeout_to.len == 0) "" else static(&STEPS, st.wait_timeout_to) orelse return null,
                        };
                        run.progress = static(&PROGRESS, st.progress) orelse return null;
                        run.status = if (st.wait == .none) .running else .waiting;
                        if (ends) |s| {
                            const output: ?usize = if (st.has_output) indexOf(&OUTPUTS, st.output) orelse return null else null;
                            endL(l, r, &run, s, output, stamp_ms, &want, sends);
                        }
                        l.runs[r] = run;
                    },
                    .start => {
                        const s = rm.Start.parse(cmd.value) orelse return null;
                        const w = indexOf(&NAMES, s.workflow) orelse return null;
                        const ev = indexOf(&EVENTS, s.event) orelse return null;
                        const k: ?usize = if (s.idem) |key| indexOf(&IDEMS, key) orelse return null else null;
                        // The server's key on a start the server doesn't make:
                        // refused first.
                        if (k) |ki| if (IDEMS[ki][0] == '_' and ev != SCHEDULE and ev != TRIGGER) return refuse(.start, "its key is the server's");
                        const at: ?PlaceL = if (s.trigger) |t| .{
                            .ns = t.ns,
                            .made = t.made,
                            .stream = indexOf(&T_STREAMS, t.stream) orelse return null,
                            .ts = t.ts,
                            .seq = t.seq,
                        } else null;
                        // A key that started a run answers with it; a
                        // trigger's start read again still moves its place.
                        if (k) |ki| if (l.idem[w][ki] != null) {
                            if (at) |p| {
                                var per = l.per_def[w] orelse PerDefL{ .active = 0, .fired_ms = 0 };
                                per.place = placeAfter(per.place, p);
                                l.per_def[w] = per;
                            }
                            return want;
                        };
                        if (l.runs[r] != null) return refuse(.start, "its run exists");
                        const disabled = if (defs.ledger[w]) |d| !d.enabled else false;
                        if (disabled) {
                            if (ev == CHILD) {
                                const f = l.parents[r] orelse return want;
                                if (f.started) return want;
                                want.sends[0] = .{ .entry_type = .call_reply, .to = PARENT_SLOTS[f.slot], .run = RUNS[r], .answer = .refused, .why = DISABLED_WHY };
                                l.parents[r] = null;
                                return want;
                            }
                            if (ev != TRIGGER) return refuse(.start, "its workflow is disabled");
                        }
                        l.runs[r] = .{ .wf = w, .status = s.status, .stepped = false, .idem = k, .created_ms = stamp_ms, .completed_ms = if (s.status.isTerminal()) stamp_ms else 0, .hist_next = 1, .started = ev };
                        if (k) |ki| l.idem[w][ki] = r;
                        var per = l.per_def[w] orelse PerDefL{ .active = 0, .fired_ms = 0 };
                        if (!s.status.isTerminal()) per.active += 1;
                        if (ev == SCHEDULE) if (k) |ki| if (SLOT_OF[ki]) |slot| {
                            per.fired_ms = @max(per.fired_ms, slot);
                        };
                        if (at) |p| per.place = placeAfter(per.place, p);
                        l.per_def[w] = per;
                        if (ev == CHILD) if (l.parents[r]) |f| if (!f.started) {
                            l.parents[r].?.started = true;
                            want.sends[0] = .{ .entry_type = .call_reply, .to = PARENT_SLOTS[f.slot], .run = RUNS[r], .answer = .started };
                        };
                    },
                    .complete => {
                        const c = rm.Complete.parse(cmd.value) orelse return null;
                        var run = l.runs[r] orelse return null;
                        if (run.status.isTerminal()) {
                            if (run.status == c.status) return want;
                            return refuse(.complete, "its run ended otherwise");
                        }
                        const output: ?usize = if (c.output) |o| indexOf(&OUTPUTS, o) orelse return null else null;
                        run.ended_at = run.hist_next;
                        run.hist_next += 1;
                        endL(l, r, &run, c.status, output, stamp_ms, &want, 0);
                        l.runs[r] = run;
                    },
                    .signal => {
                        const sg = rm.Signal.decodeAll(cmd.value, &diag) catch return null;
                        const ty = indexOf(&SIG_TYPES, sg.type) orelse return null;
                        const pl = indexOf(&SIG_PAYLOADS, sg.payload) orelse return null;
                        var run = l.runs[r] orelse return refuseAs(.signal, "its run isn't there", .signal_to_unknown);
                        if (run.status.isTerminal()) return refuseAs(.signal, "its run has ended", .signal_to_ended);
                        if (run.held_n >= SIGNAL_BOUND) return refuseAs(.signal, "its run holds as many signals as it may", .signal_full);
                        run.held[run.held_n] = .{ .type = ty, .payload = pl, .seq = run.sig_next, .at_ms = stamp_ms };
                        run.held_n += 1;
                        run.sig_next += 1;
                        run.hist[run.hist_next % HIST_RING] = .{ .kind = .received, .type = @intCast(ty), .at_ms = stamp_ms };
                        run.hist_next += 1;
                        // Its type is the one the run waits on: it wakes.
                        if (run.wait.wait == .signal and std.mem.eql(u8, run.wait.on, SIG_TYPES[ty])) {
                            run.hist[run.hist_next % HIST_RING] = .{ .kind = .matched, .type = @intCast(ty), .at_ms = stamp_ms };
                            run.hist_next += 1;
                            run.wait = .{};
                            run.status = .running;
                            run.rev += 1;
                            run.woken_rev = run.rev;
                        }
                        l.runs[r] = run;
                    },
                    else => unreachable,
                }
            },
        }
        return want;
    }

    /// The earliest signal of type `ty` the run holds, past those `picked`.
    fn earliestHeld(run: *const RunL, ty: usize, picked: []const usize) ?usize {
        for (run.held[0..run.held_n], 0..) |h, i| {
            if (h.type == ty and std.mem.indexOfScalar(usize, picked, i) == null) return i;
        }
        return null;
    }

    /// The call an answer, retry or clear names: its workflow's slot owner
    /// and its callee, null for one no step records.
    const CallAt = struct { slot: u16, at: ?struct { so: usize, c: usize } };

    fn callAt(e: *const entry_mod.Entry, kind: Kind) ?CallAt {
        var diag: schema.Diagnostic = .{};
        const slot: u16, const run: []const u8 = switch (kind) {
            .reply => blk: {
                const p = ae.Reply.decodeAll(e.payload, &diag) catch return null;
                break :blk .{ p.slot, p.run };
            },
            .done => blk: {
                const p = ae.Done.decodeAll(e.payload, &diag) catch return null;
                break :blk .{ p.slot, p.run };
            },
            .retry => blk: {
                const p = ae.CallRetry.decodeAll(e.payload, &diag) catch return null;
                break :blk .{ p.slot, p.run };
            },
            .clear => blk: {
                const p = ae.CallClear.decodeAll(e.payload, &diag) catch return null;
                break :blk .{ p.slot, p.run };
            },
            else => unreachable,
        };
        const so = slotOwnerOf(slot) orelse return .{ .slot = slot, .at = null };
        const c = indexOf(&CALLEES, run) orelse return .{ .slot = slot, .at = null };
        return .{ .slot = slot, .at = .{ .so = so, .c = c } };
    }

    /// How a call moves. Its step records it sent. Its callee's reply
    /// answers only a sent call: the run is there (started), no action
    /// took it (no action), or it has no run and won't (refused, saying
    /// why when the reply does). A call settles once, so its run's end
    /// moves it to done only from sent or started; one done, refused or
    /// with no action stays as it is. A retry asks again only for a call no
    /// action took, sending its start again. A clear drops it. Every move
    /// stamps the entry's time; an answer for a call that isn't there
    /// moves nothing.
    fn predictCall(self: *WorkflowRuns, e: *const entry_mod.Entry, kind: Kind) ?Want {
        var want: Want = .{};
        var diag: schema.Diagnostic = .{};
        const at = callAt(e, kind) orelse return null;
        const stamp = e.header.timestamp_ns;
        const held: ?*?CallL = if (at.at) |x| &self.ledger.calls[x.so][x.c] else null;
        // A moved answer is refused whatever it names: nothing sends one.
        if (kind == .reply) if ((ae.Reply.decodeAll(e.payload, &diag) catch return null).answer == .moved) return refuse(.reply, "no answer is moved yet");
        const slot = held orelse return want;
        if (kind == .clear) {
            slot.* = null;
            return want;
        }
        const c: *CallL = if (slot.*) |*x| x else return want;
        switch (kind) {
            .reply => {
                if (c.state != .sent) return want;
                const p = ae.Reply.decodeAll(e.payload, &diag) catch return null;
                c.state = switch (p.answer) {
                    .started => .started,
                    .no_action => .no_action,
                    .over_budget => .over_budget,
                    else => .refused,
                };
                if (c.state == .over_budget) want.classes.insert(.call_held);
                if (p.why) |w| c.outcome = static(&WHYS, w) orelse return null;
            },
            .done => {
                const open = c.state == .sent or c.state == .started;
                if (!open) return want;
                const p = ae.Done.decodeAll(e.payload, &diag) catch return null;
                c.state = .done;
                c.status = @backingInt(p.status);
                c.outcome = if (p.outcome.len == 0) "" else static(&DONE_OUTCOMES, p.outcome) orelse return null;
                c.result = if (p.result.len == 0) "" else static(&RESULTS, p.result) orelse return null;
            },
            .retry => {
                if (c.state != .no_action and c.state != .over_budget) return want;
                if (c.state == .over_budget) want.classes.insert(.held_call_retried);
                c.state = .sent;
                c.attempts += 1;
                want.sends[0] = startOf(c.*, at.at.?.c);
            },
            else => unreachable,
        }
        c.last_ns = stamp;
        return want;
    }

    /// The run `e` names; null for a call's answer, which names its callee.
    fn runOf(e: *const entry_mod.Entry, kind: Kind) ?usize {
        var diag: schema.Diagnostic = .{};
        return switch (kind) {
            .child_start => indexOf(&RUNS, (ae.ChildStart.decodeAll(e.payload, &diag) catch return null).run),
            .child_refuse => indexOf(&RUNS, (ae.ChildRefuse.decodeAll(e.payload, &diag) catch return null).run),
            .reply, .done, .retry, .clear => null,
            else => indexOf(&RUNS, (entry_mod.CommandPayload.deserialize(e.payload) orelse return null).key[4..]),
        };
    }

    fn kindOf(t: EntryType) Kind {
        return switch (t) {
            .workflow_start => .start,
            .workflow_step => .step,
            .workflow_complete => .complete,
            .child_start => .child_start,
            .call_reply => .reply,
            .call_done => .done,
            .step_call_retry => .retry,
            .step_call_clear => .clear,
            .workflow_signal => .signal,
            else => .child_refuse,
        };
    }

    /// Apply `e` as the shard does, on the definitions' `store`, then check
    /// it against the ledger. A refusal must leave every family as it was.
    pub fn apply(self: *WorkflowRuns, store: *store_mod.Store, defs: *const defs_family.WorkflowDefs, e: *const entry_mod.Entry) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        const kind = kindOf(@fromBackingInt(e.header.entry_type));
        const want = self.predict(defs, e, kind) orelse return error.Halt;
        var before: [FAMILIES.len + 1]u64 = undefined;
        for (FAMILIES, before[0..FAMILIES.len]) |fam, *b| b.* = store.families[fam].commits;
        before[FAMILIES.len] = store.families[channels.OutFam.ID].commits;
        const seqs = nextSeqs(store);
        var out: persistence.Result = .{};
        out.reset(e);
        try machine_mod.applyEntry(&rm.machine, store, &self.scratch, e, &out);
        // The history it sent, delivered at once, as a shard's driver
        // delivers its own, and checked as it lands.
        const landed = try self.landed.deliver(store, &self.scratch, e);
        var effect: Effect = .{ .kind = kind, .refused = out.refused() != null, .classes = want.classes };
        if (effect.refused) {
            for (FAMILIES, before[0..FAMILIES.len]) |fam, b| if (b != store.families[fam].commits) return error.RefusalChangedState;
            if (before[FAMILIES.len] != store.families[channels.OutFam.ID].commits) return error.RefusalChangedState;
            if (want.refused == null) effect.broke = REFUSED_APPLIES[@backingInt(kind)];
        } else if (want.refused) |why| effect.broke = why;
        if (effect.broke == null) effect.broke = landed;
        if (effect.broke == null) effect.broke = sentRule(store, seqs, want.sends);
        // The whole store at the indices the checksums are sampled at; the
        // run the entry names, and the calls, at every one.
        if (effect.broke == null) effect.broke = self.rule(store, runOf(e, kind), e.header.index % SWEEP_EVERY == 0);
        return effect;
    }

    /// The groups a message's slot can be in, under the store's place.
    const GROUPS = 2;

    /// Each group's next outbox seq: the outbox is never drained here, so
    /// an entry's messages are found by these, not by a walk.
    fn nextSeqs(store: *store_mod.Store) [GROUPS]u64 {
        var out: [GROUPS]u64 = undefined;
        for (&out, 0..) |*seq, g| {
            var kb: [channels.CHAN_KEY]u8 = undefined;
            const got = (view_mod.ReadView{ .store = store }).get(channels.MetaFam, channels.chanKey(&kb, @intCast(g), .main)) catch null;
            seq.* = if (got) |m| m.v.next_seq else 1;
        }
        return out;
    }

    /// The messages the entry left in the outbox, against the ledger's.
    fn sentRule(store: *store_mod.Store, before: [GROUPS]u64, wanted: [2]?Send) ?[]const u8 {
        var got: [2]?channels.Out.Value = @splat(null);
        var n: usize = 0;
        const after = nextSeqs(store);
        // In the order sent: a group's seqs are, but two groups' aren't
        // ordered between them, so each message is matched to the ledger's
        // by its type.
        for (before, after, 0..) |from, to, g| for (from..to) |seq| {
            var kb: [channels.OUT_KEY]u8 = undefined;
            const item = ((view_mod.ReadView{ .store = store }).get(channels.OutFam, channels.outKey(&kb, @intCast(g), .main, seq)) catch null) orelse
                return "an entry's message is missing from the outbox";
            if (n >= got.len) return "an entry sent a parent more than the ledger's messages";
            got[n] = item.v;
            n += 1;
        };
        var matched: [2]bool = @splat(false);
        for (got[0..n]) |maybe| {
            const m = maybe.?;
            const i = for (wanted, 0..) |w, i| {
                if (w != null and !matched[i] and m.entry_type == @backingInt(w.?.entry_type)) break i;
            } else return "an entry sent a parent a message the ledger doesn't";
            matched[i] = true;
            if (whyNot(m, wanted[i].?)) |why| return why;
        }
        for (wanted, matched) |w, m| if (w != null and !m) return "a parent the ledger tells heard nothing";
        return null;
    }

    fn whyNot(m: channels.Out.Value, w: Send) ?[]const u8 {
        if (m.slot != w.to) return "a parent's message is not the ledger's";
        var diag: schema.Diagnostic = .{};
        switch (w.entry_type) {
            .call_reply => {
                const rp = ae.Reply.decodeAll(m.payload, &diag) catch return "a parent's reply doesn't decode";
                if (!std.mem.eql(u8, rp.run, w.run) or rp.answer != w.answer.?) return "a parent's reply is not the ledger's";
                if (w.why) |why| if (rp.why == null or !std.mem.eql(u8, rp.why.?, why)) return "a parent's refusal doesn't say why";
            },
            .call_done => {
                const d = ae.Done.decodeAll(m.payload, &diag) catch return "a parent's call_done doesn't decode";
                if (!std.mem.eql(u8, d.run, w.run) or d.status != w.done.? or !std.mem.eql(u8, d.result, w.result)) return "a parent's call_done is not the ledger's";
            },
            .action_invoke => {
                const p = ae.Invoke.decodeAll(m.payload, &diag) catch return "a call's start doesn't decode";
                if (!std.mem.eql(u8, p.run, w.run) or !std.mem.eql(u8, p.action, ACTION_TARGET) or p.caller_run == null or
                    !std.mem.eql(u8, p.caller_run.?, w.caller)) return "a call's start is not the ledger's";
            },
            .child_start => {
                const p = ae.ChildStart.decodeAll(m.payload, &diag) catch return "a call's start doesn't decode";
                if (!std.mem.eql(u8, p.run, w.run) or !std.mem.eql(u8, p.workflow, CHILD_TARGET) or
                    !std.mem.eql(u8, p.parent_run, w.caller) or p.parent_slot != w.parent_slot) return "a call's start is not the ledger's";
            },
            else => unreachable,
        }
        return null;
    }

    /// The families against the ledger, and the invariants that hold of the
    /// store alone; a run's history and step outputs only for `touched`, the
    /// one run the entry names.
    fn rule(self: *WorkflowRuns, store: *store_mod.Store, touched: ?usize, sweep: bool) ?[]const u8 {
        return self.ruleOrOom(store, touched, sweep) catch "out of memory checking the runs";
    }

    fn ruleOrOom(self: *WorkflowRuns, store: *store_mod.Store, touched: ?usize, sweep: bool) error{OutOfMemory}!?[]const u8 {
        _ = self.scratch.reset(.retain_capacity);
        const a = self.scratch.allocator();
        const rv: view_mod.ReadView = .{ .store = store };
        const l = &self.ledger;

        if (try self.callsRule(store, a)) |why| return why;
        if (try hm.ruleOf(hm.Workflows, rv, a)) |why| return why;
        if (!sweep) return if (touched) |t| self.runRule(store, a, t, true) else null;
        // On the store alone: counts, idempotency keys naming their run, and
        // a run's wait agreeing with its status.
        var active: [NAMES.len]u32 = @splat(0);
        var runs = rv.range(rm.RunFam, "");
        var stored: usize = 0;
        while (runs.next() catch return "a run could not be read") |item| : (stored += 1) {
            const v = item.value.v;
            const w = indexOf(&NAMES, v.workflow) orelse return "a run of a workflow the workload never names";
            const s = std.enums.fromInt(RunStatus, v.status) orelse return "a run's status is not a status";
            if (!s.isTerminal()) active[w] += 1;
            if (if (s.isTerminal()) v.wait != .none else (s == .waiting) != (v.wait != .none)) return "a run's status and its wait disagree";
        }
        for (NAMES, active, l.per_def) |name, n, want| {
            const got = rm.perDefOf(rv, view_mod.idKey(try a.alloc(u8, 4 + name.len), NS, name) catch unreachable);
            if ((if (got) |g| g.active else 0) != n) return "a workflow's count is not its runs not ended";
            if ((got == null) != (want == null)) return "a workflow's record is not the ledger's";
            if (got) |g| if (g.active != want.?.active or g.fired_ms != want.?.fired_ms) return "a workflow's count or fired slot is not the ledger's";
            if (got) |g| if (!samePlace(g, want.?.place)) return "a workflow's trigger place is not the ledger's";
        }
        var idems = rv.range(rm.IdemFam, "");
        var held_idems: usize = 0;
        while (idems.next() catch return "an idempotency key could not be read") |item| : (held_idems += 1) {
            const run = rm.runOf(rv, view_mod.idKey(try a.alloc(u8, 4 + item.value.v.run.len), NS, item.value.v.run) catch unreachable) orelse
                return "an idempotency key names no run";
            const want = try rm.idemKey(a, NS, run.workflow, run.idempotency_key);
            if (!std.mem.eql(u8, want, item.key)) return "an idempotency key names a run of another workflow or key";
        }

        // Against the ledger, run by run.
        var want_runs: usize = 0;
        var want_idems: usize = 0;
        for (l.idem) |row| for (row) |x| {
            if (x != null) want_idems += 1;
        };
        if (held_idems != want_idems) return "the idempotency keys are not the ledger's";
        for (0..RUNS.len) |r| {
            if (try self.runRule(store, a, r, r == touched)) |why| return why;
            if (l.runs[r] != null) want_runs += 1;
        }
        if (stored != want_runs) return "the runs are not the ledger's";
        return null;
    }

    /// `step.calls` against the ledger, and on the store alone: each call's
    /// caller has ended or waits on it. Few enough to check at every entry.
    fn callsRule(self: *WorkflowRuns, store: *store_mod.Store, a: Allocator) error{OutOfMemory}!?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = store };
        const l = &self.ledger;
        var it = rv.range(calls_mod.Calls, "");
        var held: usize = 0;
        while (it.next() catch return "a call could not be read") |item| : (held += 1) {
            const k = item.key;
            if (k.len < 6) return "a call's key is not (slot, callee)";
            const callee = k[6..];
            const so = slotOwnerOf(std.mem.readInt(u16, k[4..6], .big)) orelse return "a call is recorded under a slot no workflow's runs are on";
            const c = indexOf(&CALLEES, callee) orelse return "a call is recorded of a callee no step names";
            const want = l.calls[so][c] orelse return "a call the ledger doesn't hold is recorded";
            const v = item.value.v;
            if (v.kind != want.kind or !std.mem.eql(u8, v.caller_run, RUNS[want.caller]) or !std.mem.eql(u8, v.caller_workflow, NAMES[want.wf]) or
                !std.mem.eql(u8, v.step, STEPS[want.step]) or !std.mem.eql(u8, v.target, targetOf(want.kind)) or v.can_wait != want.can_wait)
                return "a call's record is not its step's";
            if (v.state != want.state or v.attempts != want.attempts or v.first_ns != want.first_ns or v.last_ns != want.last_ns or
                v.status != want.status or !std.mem.eql(u8, v.outcome, want.outcome) or !std.mem.eql(u8, v.result, want.result))
                return "a call's state is not the ledger's";
            const caller = rm.runOf(rv, view_mod.idKey(try a.alloc(u8, 4 + v.caller_run.len), NS, v.caller_run) catch unreachable) orelse
                return "a call's caller run is gone";
            const s = std.enums.fromInt(RunStatus, caller.status) orelse return "a run's status is not a status";
            if (!s.isTerminal() and !(caller.wait == .call and std.mem.eql(u8, caller.wait_on, callee))) return "a call is recorded that its caller doesn't wait on";
        }
        var want_held: usize = 0;
        for (l.calls) |row| for (row) |x| {
            if (x != null) want_held += 1;
        };
        if (held != want_held) return "the calls are not the ledger's";
        return null;
    }

    /// Run `r` against the ledger: its record, its parent's, its workflow's
    /// count, its idempotency key, and if `walk`, its history and step
    /// outputs.
    fn runRule(self: *WorkflowRuns, store: *store_mod.Store, a: Allocator, r: usize, walk: bool) error{OutOfMemory}!?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = store };
        const l = &self.ledger;
        const name = RUNS[r];
        const want = l.runs[r];
        const parent = l.parents[r];
        const key = view_mod.idKey(try a.alloc(u8, 4 + name.len), NS, name) catch unreachable;
        const got = rm.runOf(rv, key);
        const pgot = rm.parentOf(rv, NS, name);
        if ((pgot == null) != (parent == null)) return "a child's parent record is not the ledger's";
        if (parent) |p| {
            const g = pgot.?;
            if (g.parent_slot != PARENT_SLOTS[p.slot] or !std.mem.eql(u8, g.parent_run, PARENT_RUNS[p.parent_run])) return "a child's parent is not the ledger's";
            if ((g.state == .started) != p.started) return "whether a child started is not the ledger's";
            if (g.state == .started and got == null) return "a started child's run is gone";
        }
        const prefix = try rm.runRange(a, NS, name);
        const run = want orelse {
            if (got != null) return "a run the ledger doesn't hold is stored";
            var none = rv.range(rm.SigFam, prefix);
            if ((none.next() catch return "a signal could not be read") != null) return "a run the ledger doesn't hold has signals";
            return null;
        };
        const g = got orelse return "a run the ledger holds is gone";
        if (!std.mem.eql(u8, g.workflow, NAMES[run.wf])) return "a run's workflow is not its start's";
        if (g.status != @backingInt(run.status)) return "a run's status is not the ledger's";
        if (run.woken_rev != null and g.rev != run.rev) return "a woken run's rev is not the ledger's";
        if ((g.rev > 0) != run.stepped or g.rev != run.rev) return "whether a run's first step was taken is not the ledger's";
        if (g.created_at_ms != run.created_ms or g.completed_at_ms != run.completed_ms) return "a run's stamps are not the ledger's";
        if (!std.mem.eql(u8, g.idempotency_key, if (run.idem) |k| IDEMS[k] else "")) return "a run's idempotency key is not its start's";
        if ((g.has_output != 0) != (run.output != null) or !std.mem.eql(u8, g.output, if (run.output) |o| OUTPUTS[o] else "")) return "a run's output is not its completion's";
        if (!std.mem.eql(u8, g.step, if (run.step) |s| STEPS[s] else "")) return "a run's step is not its last step's";
        const w = run.wait;
        if (g.wait != w.wait or g.wait_kind != w.kind or g.wait_slot != w.slot or !std.mem.eql(u8, g.wait_on, w.on) or
            g.wait_due_ms != w.due_ms or !std.mem.eql(u8, g.wait_timeout_to, w.timeout_to)) return "a run's wait is not its last step's";
        if (!std.mem.eql(u8, g.progress, run.progress)) return "a run's counters are not its last step's";
        if (g.hist_next != run.hist_next) return "a run's history counts are not the ledger's";
        if (g.hist_flushed > g.hist_next or g.hist_pending.len > rm.HIST_CHUNK) return "a run's pending history is past its bounds";
        if (run.status.isTerminal() and (g.hist_flushed != g.hist_next or g.hist_pending.len != 0)) return "an ended run holds history it hasn't sent";
        if (g.created_ns / std.time.ns_per_ms != @as(u64, @intCast(run.created_ms))) return "a run's incarnation is not its start's";
        // Ended, its history is one row, found from its summary alone: what
        // a delete's drop is built from.
        if (run.status.isTerminal()) {
            const at: u64 = @intCast(@max(g.completed_at_ms, 0));
            if (hm.endedRow(rv, a, .workflow, NS, g.workflow, name, g.created_ns, at) == null) return "an ended run's history row is not where its summary says";
            var live = try hm.rowsOf(rv, a, .workflow, NS, g.workflow, name, g.created_ns, null);
            if (live.next() != null) return "an ended run keeps live history rows";
        }
        // Its workflow's record, whatever the sweep finds of the rest.
        const wname = NAMES[run.wf];
        const per = rm.perDefOf(rv, view_mod.idKey(try a.alloc(u8, 4 + wname.len), NS, wname) catch unreachable);
        const per_want = l.per_def[run.wf].?;
        if (per == null or per.?.active != per_want.active or per.?.fired_ms != per_want.fired_ms) return "a workflow's count or fired slot is not the ledger's";
        if (!samePlace(per.?, per_want.place)) return "a workflow's trigger place is not the ledger's";
        if (run.idem) |k| {
            const got_idem = (rv.get(rm.IdemFam, try rm.idemKey(a, NS, wname, IDEMS[k])) catch return "an idempotency key could not be read") orelse return "the idempotency keys are not the ledger's";
            if (!std.mem.eql(u8, got_idem.v.run, RUNS[l.idem[run.wf][k].?])) return "the idempotency keys are not the ledger's";
        }
        // Its signals, at every entry naming it.
        if (run.status.isTerminal()) {
            var after = rv.range(rm.SigFam, prefix);
            if ((after.next() catch return "a signal could not be read") != null or g.sig_held != 0) return "an ended run holds signals";
        }
        if (g.sig_next != run.sig_next) return "a run's signal seq is not the ledger's";
        if (g.sig_held != run.held_n) return "a run's held signal count is not the ledger's";
        var sigs = rv.range(rm.SigFam, prefix);
        var si: usize = 0;
        while (sigs.next() catch return "a signal could not be read") |item| : (si += 1) {
            if (si >= run.held_n) return "a run holds signals the ledger doesn't";
            const h = run.held[si];
            const v = item.value.v;
            if (std.mem.readInt(u64, item.key[item.key.len - 8 ..][0..8], .big) != h.seq or !std.mem.eql(u8, v.type, SIG_TYPES[h.type]) or
                !std.mem.eql(u8, v.payload, SIG_PAYLOADS[h.payload]) or v.at_ms != h.at_ms) return "a run's held signals are not the ledger's";
        }
        if (si != run.held_n) return "a run's held signals are not the ledger's";
        if (!walk) return null;
        // Its pending events: those since what it sent, each its own.
        var hd: schema.Diagnostic = .{};
        var hr: schema.Reader = .{ .bytes = g.hist_pending, .diag = &hd };
        var seq = g.hist_flushed;
        if (run.hist_next - seq > HIST_RING) return "a run holds more pending history than a chunk";
        while (hr.pos < g.hist_pending.len) : (seq += 1) {
            const h = rm.HistRec.decode(&hr, "event") catch return "a run's pending history doesn't decode";
            if (seq >= run.hist_next) return "a run holds more history than it made";
            if (h.seq != seq) return "a run's pending history is not its next seqs";
            if (seq == 0) {
                if (!std.mem.eql(u8, h.event, EVENTS[run.started]) or !std.mem.eql(u8, h.detail, INPUT) or h.at_ms != run.created_ms) return "a run's first event is not its start's";
            } else if (run.ended_at == seq) {
                if (!std.mem.eql(u8, h.event, CANCEL_EVENT) or h.at_ms != run.completed_ms) return "a run's last event is not its cancel's";
            } else switch (run.hist[seq % HIST_RING].kind) {
                .step => if (!std.mem.eql(u8, h.event, STEP_EVENT) or h.at_ms != 0 or h.detail.len != 0) return "a run's history is not its steps'",
                .received, .matched => |k| {
                    const hl = run.hist[seq % HIST_RING];
                    const event = if (k == .received) "signal_received" else "signal_matched";
                    if (!std.mem.eql(u8, h.event, event) or !std.mem.eql(u8, h.detail, SIG_TYPES[hl.type]) or h.at_ms != hl.at_ms) return "a run's history is not its signals'";
                },
            }
        }
        if (seq != run.hist_next) return "a run's pending history is missing events";
        var outs = rv.range(rm.OutFam, prefix);
        var got_outs: [STEPS.len]?usize = @splat(null);
        while (outs.next() catch return "a step output could not be read") |item| {
            const step = indexOf(&STEPS, item.key[prefix.len..]) orelse return "a step output under a step no step names";
            const o = indexOf(&OUTCOMES, item.value.v.outcome) orelse return "a step's outcome is not its step's";
            if (!std.mem.eql(u8, item.value.v.output, OUTPUTS[o])) return "a step's output is not its step's";
            got_outs[step] = o;
        }
        if (!std.mem.eql(?usize, &got_outs, &run.outs)) return "a run's step outputs are not its steps'";
        return null;
    }

    pub fn checksums(store: *store_mod.Store) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(store, fam);
        return sums;
    }
};

pub const Made = struct { entry_type: EntryType, payload: []const u8 };

/// A runs entry, as the workflows handler, a parent's call and a call's
/// callee make them, from the leader's own applied ledger: starts of every
/// kind (some of a disabled workflow, some repeating a key or a run), steps
/// at a run's rev (some at one another leader moved past, some carrying
/// enough events that a run's history passes what it keeps, some waiting
/// or calling, some taking a call's end before it has one), child calls
/// and refusals, a call's answers, and cancels of runs it holds.
pub fn make(r: std.Random, buf: []u8, leader: *const WorkflowRuns) ?Made {
    const l = &leader.ledger;
    // The runs as the leader holds them, so most entries find what they
    // name: a free id for a start, a pending parent for its child's start,
    // a run not yet stepped or ended for a step or a completion.
    var free: Pick = .{};
    var live: Pick = .{};
    var unstepped: Pick = .{};
    var held: Pick = .{};
    var pending: Pick = .{};
    for (l.runs, l.parents, 0..) |x, p, i| {
        if (x) |run| {
            held.add(i);
            if (!run.status.isTerminal()) live.add(i);
            if (!run.status.isTerminal() and !run.stepped) unstepped.add(i);
        } else {
            if (p == null) free.add(i);
            if (p != null and !p.?.started) pending.add(i);
        }
    }
    const any = r.uintLessThan(usize, RUNS.len);
    var kb: [16]u8 = undefined;
    const roll = r.uintLessThan(u8, 100);
    // The live run holding the most signals: pressed on until it holds as
    // many as it may, and mostly spared ending and taking meanwhile; once
    // full, it is a run like any other.
    var most: ?usize = null;
    for (live.ids[0..live.n]) |i| {
        if (most == null or l.runs[i].?.held_n > l.runs[most.?].?.held_n) most = i;
    }
    const most_held: usize = if (most) |m| l.runs[m].?.held_n else 0;
    const hoard: ?usize = if (most_held >= 8 and most_held < SIGNAL_BOUND) most else null;
    if (roll < 12) {
        const run = free.or_(r, 50, any);
        const p = ae.ChildStart.encodeInto(buf, .{
            .ns = NS.int(),
            .parent_slot = PARENT_SLOTS[r.uintLessThan(usize, PARENT_SLOTS.len)],
            .parent_run = PARENT_RUNS[r.uintLessThan(usize, PARENT_RUNS.len)],
            .parent_workflow = "pwf",
            .workflow = NAMES[r.uintLessThan(usize, NAMES.len)],
            .version = "",
            .run = RUNS[run],
            .input = INPUT,
        }) catch return null;
        return .{ .entry_type = .child_start, .payload = p };
    }
    if (roll < 17) {
        const answers = [_]ae.Answer{ .no_action, .conflict };
        const p = ae.ChildRefuse.encodeInto(buf, .{ .ns = NS.int(), .run = RUNS[pending.or_(r, 70, any)], .answer = answers[r.uintLessThan(usize, answers.len)] }) catch return null;
        return .{ .entry_type = .child_refuse, .payload = p };
    }
    // A step of a run the leader holds, at its rev, or now and then a rev
    // another leader moved past.
    if (roll < 34 and held.n > 0) {
        // Now and then the run with the longest history, so some pass
        // what a run keeps.
        var longest: ?usize = null;
        for (live.ids[0..live.n]) |i| {
            if (longest == null or l.runs[i].?.hist_next > l.runs[longest.?].?.hist_next) longest = i;
        }
        // Now and then a run waiting on a signal, or one a signal just woke:
        // a step decided from the wait may meet the moved rev.
        var waiting: Pick = .{};
        for (live.ids[0..live.n]) |i| {
            const run = l.runs[i].?;
            if (run.wait.wait == .signal or run.woken_rev == run.rev) waiting.add(i);
        }
        const pick = r.uintLessThan(u8, 100);
        const c = if (longest != null and pick < 25) longest.? else if (pick < 45) waiting.or_(r, 100, live.or_(r, 80, held.at(r))) else live.or_(r, 80, unstepped.or_(r, 30, held.at(r)));
        const vb = buf[buf.len / 2 ..];
        const spare = hoard == c and r.uintLessThan(u8, 100) < 90;
        const value = stepValue(r, vb, c, l.runs[c].?, spare) orelse return null;
        return command(buf[0 .. buf.len / 2], .workflow_step, view_mod.idKey(&kb, NS, RUNS[c]) catch unreachable, value);
    }
    // A call's answer, retry or clear: mostly of a call the leader holds.
    if (roll < 48) return callAnswer(r, buf, l);
    // A signal: a quarter of them, or most once it holds many, to the live
    // run holding the most, so its bound is reached; the rest mostly to a
    // live run, now and then to an ended or an unknown one.
    if (roll < 60) {
        // Once it holds many, mostly to it: runs end and steps take
        // signals, so the bound is reached only by pressing on one. A full
        // one gets a few more, refused.
        const k = r.uintLessThan(u8, 100);
        const press: u8 = if (hoard != null) 85 else if (most_held >= SIGNAL_BOUND) 8 else 25;
        const c = if (most != null and k < press) most.? else if (k < 85) live.or_(r, 90, held.or_(r, 50, any)) else held.or_(r, 50, any);
        // To a run waiting on a signal, mostly its type: it wakes, and a
        // step decided from the wait meets the moved rev.
        var ty = SIG_TYPES[r.uintLessThan(usize, SIG_TYPES.len)];
        if (l.runs[c]) |run| if (run.wait.wait == .signal and r.uintLessThan(u8, 100) < 60) {
            ty = run.wait.on;
        };
        var sb: [64]u8 = undefined;
        const value = rm.Signal.encodeInto(&sb, .{ .type = ty, .payload = SIG_PAYLOADS[r.uintLessThan(usize, SIG_PAYLOADS.len)] }) catch return null;
        return command(buf, .workflow_signal, view_mod.idKey(&kb, NS, RUNS[c]) catch unreachable, value);
    }
    // A completion names a run the leader holds, and its workflow.
    if (roll < 68 and held.n > 0) {
        var c = live.or_(r, 75, held.at(r));
        if (hoard == c and r.uintLessThan(u8, 100) < 95) c = held.at(r);
        const vb = buf[buf.len / 2 ..];
        const value = completeValue(r, vb, ENDS[r.uintLessThan(usize, ENDS.len)], NAMES[l.runs[c].?.wf]) orelse return null;
        return command(buf[0 .. buf.len / 2], .workflow_complete, view_mod.idKey(&kb, NS, RUNS[c]) catch unreachable, value);
    }
    const ev = r.uintLessThan(usize, EVENTS.len);
    const run = if (ev == CHILD) pending.or_(r, 70, any) else free.or_(r, 75, any);
    const idem: []const u8 = if (ev == CHILD or r.boolean()) "" else IDEMS[r.uintLessThan(usize, IDEMS.len)];
    // A trigger's start says where it read.
    const at: ?rm.TriggerAt = if (ev == TRIGGER) .{
        .ns = T_NS[r.uintLessThan(usize, T_NS.len)],
        .made = T_MADE[r.uintLessThan(usize, T_MADE.len)],
        .stream = T_STREAMS[r.uintLessThan(usize, T_STREAMS.len)],
        .ts = r.uintLessThan(u64, 4),
        .seq = r.uintLessThan(u64, 2),
    } else null;
    var vb: [160]u8 = undefined;
    return command(buf, .workflow_start, view_mod.idKey(&kb, NS, RUNS[run]) catch unreachable, startValue(&vb, NAMES[r.uintLessThan(usize, NAMES.len)], EVENTS[ev], idem, at));
}

/// A callee's answer to a call (a reply or its end), its step's retry, or
/// its clear: of a call the leader holds 80% of the time, a retry mostly of
/// one no action took.
fn callAnswer(r: std.Random, buf: []u8, l: *const WorkflowRuns.Ledger) ?Made {
    const At = struct { so: usize, c: usize };
    var calls: [NAMES.len * CALLEES.len]At = undefined;
    var no_action: [NAMES.len * CALLEES.len]At = undefined;
    var n: usize = 0;
    var na: usize = 0;
    for (l.calls, 0..) |row, so| for (row, 0..) |x, c| if (x) |call| {
        calls[n] = .{ .so = so, .c = c };
        n += 1;
        if (call.state == .no_action or call.state == .over_budget) {
            no_action[na] = .{ .so = so, .c = c };
            na += 1;
        }
    };
    const k = r.uintLessThan(u8, 100);
    var at: At = .{ .so = r.uintLessThan(usize, NAMES.len), .c = r.uintLessThan(usize, CALLEES.len) };
    if (k >= 70 and k < 90 and na > 0 and r.uintLessThan(u8, 100) < 70) {
        at = no_action[r.uintLessThan(usize, na)];
    } else if (n > 0 and r.uintLessThan(u8, 100) < 80) at = calls[r.uintLessThan(usize, n)];
    const slot = slotOf(NAMES[at.so]);
    const run = CALLEES[at.c];
    if (k < 35) {
        const answers = [_]ae.Answer{ .started, .started, .started, .no_action, .no_action, .over_budget, .conflict, .refused, .skipped };
        // A moved answer is refused: nothing sends one until slots move.
        const answer: ae.Answer = if (r.uintLessThan(u8, 100) < 3) .moved else answers[r.uintLessThan(usize, answers.len)];
        const why: ?[]const u8 = if (r.uintLessThan(u8, 100) < 30) WHYS[0] else null;
        const p = ae.Reply.encodeInto(buf, .{ .ns = NS.int(), .slot = slot, .run = run, .answer = answer, .why = why }) catch return null;
        return .{ .entry_type = .call_reply, .payload = p };
    }
    if (k < 70) {
        const statuses = std.enums.values(ae.DoneStatus);
        const p = ae.Done.encodeInto(buf, .{
            .ns = NS.int(),
            .slot = slot,
            .run = run,
            .status = statuses[r.uintLessThan(usize, statuses.len)],
            .outcome = DONE_OUTCOMES[r.uintLessThan(usize, DONE_OUTCOMES.len)],
            .result = RESULTS[r.uintLessThan(usize, RESULTS.len)],
        }) catch return null;
        return .{ .entry_type = .call_done, .payload = p };
    }
    if (k < 90) {
        const p = ae.CallRetry.encodeInto(buf, .{ .ns = NS.int(), .slot = slot, .run = run }) catch return null;
        return .{ .entry_type = .step_call_retry, .payload = p };
    }
    const p = ae.CallClear.encodeInto(buf, .{ .ns = NS.int(), .slot = slot, .run = run }) catch return null;
    return .{ .entry_type = .step_call_clear, .payload = p };
}

/// Run ids to pick from.
const Pick = struct {
    ids: [RUNS.len]usize = undefined,
    n: usize = 0,

    fn add(self: *Pick, i: usize) void {
        self.ids[self.n] = i;
        self.n += 1;
    }

    fn at(self: *const Pick, r: std.Random) usize {
        return self.ids[r.uintLessThan(usize, self.n)];
    }

    /// One of these, `percent` of the time when there are any; else `other`.
    fn or_(self: *const Pick, r: std.Random, percent: u8, other: usize) usize {
        if (self.n == 0 or r.uintLessThan(u8, 100) >= percent) return other;
        return self.at(r);
    }
};

fn command(buf: []u8, t: EntryType, key: []const u8, value: []const u8) ?Made {
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = NS, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
    const n = cmd.serialize(buf) orelse return null;
    return .{ .entry_type = t, .payload = buf[0..n] };
}

fn startValue(buf: []u8, wf: []const u8, event: []const u8, idem: []const u8, at: ?rm.TriggerAt) []const u8 {
    return (rm.Start{ .workflow = wf, .version = "1", .status = .running, .event = event, .idem = if (idem.len > 0) idem else null, .trigger = at, .input = INPUT }).encode(buf).?;
}

/// Whether the machine's place is the ledger's (none: nothing recorded).
fn samePlace(g: rm.PerDef.Value, want: ?WorkflowRuns.PlaceL) bool {
    const p = want orelse return g.trig_ns == 0 and g.trig_made == 0 and g.trig_stream.len == 0 and g.trig_ts == 0 and g.trig_seq == 0;
    return g.trig_ns == p.ns and g.trig_made == p.made and std.mem.eql(u8, g.trig_stream, T_STREAMS[p.stream]) and g.trig_ts == p.ts and g.trig_seq == p.seq;
}

/// A step of run `c` at its rev (now and then the next one): some events,
/// now and then nearly a step's most, so a run's history passes what it
/// keeps; maybe some step outputs. A run waiting on a call mostly takes
/// its end (refused while the call hasn't one), or keeps waiting, ends, or
/// now and then leaves it unsettled or takes another call's end (both
/// refused). Any other run waits for nothing, a call, a signal or a timer,
/// or ends.
/// A `spare` step neither ends its run nor takes its signals.
fn stepValue(r: std.Random, buf: []u8, c: usize, run: WorkflowRuns.RunL, spare: bool) ?[]const u8 {
    const wf = NAMES[run.wf];
    // Long steps are rare, but likelier for a run already long: every
    // replica's checksums walk them all.
    const long_percent: u8 = if (run.hist_next >= rm.MAX_STEP_EVENTS) 50 else 4;
    const events: u32 = if (r.uintLessThan(u8, 100) < long_percent) r.intRangeAtMost(u32, 56, rm.MAX_STEP_EVENTS) else r.uintLessThan(u32, 4);
    var eb: [rm.MAX_STEP_EVENTS * 24]u8 = undefined;
    var ew: schema.Writer = .{ .buf = &eb };
    for (0..events) |_| rm.Event.encode(&ew, .{ .event = STEP_EVENT, .detail = "", .at_ms = 0 }) catch return null;
    var ob: [STEPS.len * 32]u8 = undefined;
    var ow: schema.Writer = .{ .buf = &ob };
    var outs: u32 = 0;
    for (STEPS) |step| if (r.uintLessThan(u8, 100) < 30) {
        const o = r.uintLessThan(usize, OUTCOMES.len);
        rm.StepOutput.encode(&ow, .{ .step = step, .outcome = OUTCOMES[o], .output = OUTPUTS[o] }) catch return null;
        outs += 1;
    };
    var value: rm.Step.Value = .{
        // Now and then a rev another leader moved past; for a run a signal
        // woke, often the rev from before the wake, as a step decided from
        // the wait and applied after the signal is.
        .rev = if (run.woken_rev == run.rev and r.uintLessThan(u8, 100) < 40) run.rev -% 1 else if (r.uintLessThan(u8, 100) < 10) run.rev +% 1 else run.rev,
        .step = STEPS[r.uintLessThan(usize, STEPS.len)],
        .events = .{ .bytes = ew.written(), .count = events },
        .outputs = .{ .bytes = ow.written(), .count = outs },
        .wait = .none,
        .wait_kind = .action,
        .wait_slot = 0,
        .wait_on = "",
        .wait_due_ms = 0,
        .wait_timeout_to = "",
        .progress = if (r.uintLessThan(u8, 100) < 3) &OVERSIZED else PROGRESS[r.uintLessThan(usize, PROGRESS.len)],
        .settles = "",
        .takes = .{ .bytes = "", .count = 0 },
        .caller_slot = 0,
        .workflow = wf,
        .ends = 0,
        .has_output = false,
        .output = "",
        .call = "",
    };
    var next = true;
    const roll = r.uintLessThan(u8, 100);
    if (run.wait.wait == .call) {
        if (roll < 55 or roll >= 91) {
            // Its call's end, or another callee's (refused).
            value.settles = if (roll < 55) run.wait.on else CALLEES[r.uintLessThan(usize, CALLEES.len)];
            value.caller_slot = slotOf(wf);
        } else if (roll < 70) {
            setWait(&value, run.wait);
            next = false;
        } else if (roll < 82) {
            next = false;
            if (!spare) endStep(r, &value);
        } else next = false;
    } else if (roll < 6 and !spare) {
        next = false;
        endStep(r, &value);
    }
    // Signals its waits took: mostly ones it holds, now and then one more
    // than it holds of a type, or one it holds none of.
    var tb: [SIGNAL_BOUND * 8]u8 = undefined;
    var tw: schema.Writer = .{ .buf = &tb };
    var takes: u32 = 0;
    if (!spare and r.uintLessThan(u8, 100) < 35) {
        const want_n = r.intRangeAtMost(usize, 1, 3);
        for (0..want_n) |_| {
            const ty = if (run.held_n > 0 and r.uintLessThan(u8, 100) < 85) run.held[r.uintLessThan(usize, run.held_n)].type else r.uintLessThan(usize, SIG_TYPES.len);
            rm.Take.encode(&tw, .{ .type = SIG_TYPES[ty] }) catch return null;
            takes += 1;
        }
    }
    value.takes = .{ .bytes = tw.written(), .count = takes };
    var cb: [256]u8 = undefined;
    if (next) {
        const w = r.uintLessThan(u8, 100);
        if (w < 30) {
            const kind: ae.CallKind = if (r.boolean()) .action else .workflow;
            const callee = CALLEES[r.uintLessThan(usize, CALLEES.len)];
            value.call = ae.Call.encodeInto(&cb, .{
                .ns = NS.int(),
                .kind = kind,
                .caller_run = RUNS[c],
                .caller_workflow = wf,
                .step = value.step,
                .target = targetOf(kind),
                .version = "",
                .run = callee,
                .input = INPUT,
                .can_wait = r.boolean(),
            }) catch return null;
            setWait(&value, .{ .wait = .call, .kind = kind, .slot = slotOf(targetOf(kind)), .on = callee });
        } else if (w < 47) {
            setWait(&value, .{ .wait = .signal, .on = SIG_TYPES[r.uintLessThan(usize, SIG_TYPES.len)], .due_ms = if (r.boolean()) 0 else 5000, .timeout_to = if (r.boolean()) "" else STEPS[r.uintLessThan(usize, STEPS.len)] });
        } else if (w < 62) {
            setWait(&value, .{ .wait = .timer, .due_ms = r.intRangeAtMost(i64, 1000, 9000) });
        }
    }
    return rm.Step.encodeInto(buf, value) catch null;
}

fn setWait(v: *rm.Step.Value, w: WorkflowRuns.WaitL) void {
    v.wait = w.wait;
    v.wait_kind = w.kind;
    v.wait_slot = w.slot;
    v.wait_on = w.on;
    v.wait_due_ms = w.due_ms;
    v.wait_timeout_to = w.timeout_to;
}

/// The step ends its run, maybe with an output.
fn endStep(r: std.Random, v: *rm.Step.Value) void {
    v.ends = @backingInt(ENDS[r.uintLessThan(usize, ENDS.len)]);
    v.has_output = r.boolean();
    if (v.has_output) v.output = OUTPUTS[r.uintLessThan(usize, OUTPUTS.len)];
}

/// A cancel's completion, as `Complete.parse` reads it: maybe an output.
fn completeValue(r: std.Random, buf: []u8, status: RunStatus, wf: []const u8) ?[]const u8 {
    const c: rm.Complete = .{
        .status = status,
        .output = if (r.boolean()) OUTPUTS[r.uintLessThan(usize, OUTPUTS.len)] else null,
        .event = CANCEL_EVENT,
        .detail = "",
        .workflow = wf,
    };
    return c.encode(buf);
}
